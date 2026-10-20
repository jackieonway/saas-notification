//! Bounded TCP ownership and explicit revocation below Engine.IO/WebSocket.
//!
//! Socket.IO namespace disconnect alone leaves a noncooperating Engine.IO peer
//! alive. These controls revoke the actual IO after the caller's flush window.

use axum::{
    extract::connect_info::Connected,
    serve::{IncomingStream, Listener},
};
use futures_util::task::AtomicWaker;
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
};

// 0 means an HTTP keepalive connection currently belongs to no Engine group.
// The final value is terminal, so ownership transfer cannot race a completed
// revocation and reopen an IO owner that is already being closed.
const CLOSED_OWNER: u64 = u64::MAX;
static NEXT_GROUP: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct State {
    closed: AtomicBool,
    owner: AtomicU64,
    reader: AtomicWaker,
    writer: AtomicWaker,
}

/// Cloneable connection metadata; it does not own the TCP admission permit.
#[derive(Clone, Default)]
pub(crate) struct Control(Arc<State>);

impl Control {
    /// Terminate reads and writes, including either direction currently pending.
    pub(crate) fn close(&self) {
        self.0.owner.store(CLOSED_OWNER, Ordering::Release);
        if !self.0.closed.swap(true, Ordering::AcqRel) {
            self.0.reader.wake();
            self.0.writer.wake();
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::Acquire)
            || self.0.owner.load(Ordering::Acquire) == CLOSED_OWNER
    }

    /// Detach a new HTTP request from the Engine that previously used keepalive.
    /// A terminally closed transport can never be reopened by this operation.
    pub(crate) fn detach(&self) {
        let _ = self.assign_owner(0);
    }

    fn assign_owner(&self, next: u64) -> bool {
        let mut owner = self.0.owner.load(Ordering::Acquire);
        loop {
            if owner == CLOSED_OWNER {
                return false;
            }
            match self.0.owner.compare_exchange_weak(
                owner,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(current) => owner = current,
            }
        }
    }

    fn close_if_owned_by(&self, owner: u64) {
        if self
            .0
            .owner
            .compare_exchange(owner, CLOSED_OWNER, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.close();
        }
    }
}

struct Members {
    id: u64,
    closed: bool,
    controls: Vec<Weak<State>>,
}

/// The TCP connections used by one Engine.IO session, including polling/Upgrade.
///
/// Entries are weak, deduplicated and pruned on registration. Closed streams
/// mark their control closed before releasing their global TCP permit, keeping
/// the live membership bounded by that global TCP limit.
#[derive(Clone)]
pub(crate) struct Group(Arc<Mutex<Members>>);

impl Default for Group {
    fn default() -> Self {
        let id = NEXT_GROUP
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1).filter(|next| *next < CLOSED_OWNER)
            })
            .unwrap_or(CLOSED_OWNER);
        Self(Arc::new(Mutex::new(Members {
            id,
            // Exhausting IDs must deny admission rather than reuse kill rights.
            closed: id == CLOSED_OWNER,
            controls: Vec::new(),
        })))
    }
}

impl Group {
    pub(crate) fn register(&self, control: &Control) {
        let mut members = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if members.closed {
            drop(members);
            control.close();
            return;
        }
        let id = members.id;
        members.controls.retain(|member| {
            member
                .upgrade()
                .is_some_and(|state| state.owner.load(Ordering::Acquire) == id)
        });
        if !control.assign_owner(id) {
            return;
        }
        let weak = Arc::downgrade(&control.0);
        if !members
            .controls
            .iter()
            .any(|member| Weak::ptr_eq(member, &weak))
        {
            members.controls.push(weak);
        }
    }

    /// Close all current members and immediately close any later registration.
    pub(crate) fn close(&self) {
        let (id, controls) = {
            let mut members = self.0.lock().unwrap_or_else(|error| error.into_inner());
            members.closed = true;
            (members.id, std::mem::take(&mut members.controls))
        };
        for control in controls.into_iter().filter_map(|control| control.upgrade()) {
            Control(control).close_if_owned_by(id);
        }
    }
}

#[derive(Default)]
struct Accepted {
    closed: bool,
    controls: Vec<Weak<State>>,
}

/// Service-wide shutdown authority over every accepted TCP, including peers
/// whose incomplete HTTP heads have never entered request/Engine.IO middleware.
/// Weak metadata never keeps an IO owner or its semaphore permit alive.
#[derive(Clone, Default)]
pub(crate) struct Shutdown(Arc<Mutex<Accepted>>);

impl Shutdown {
    fn register(&self, control: &Control) -> bool {
        let mut accepted = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if accepted.closed {
            drop(accepted);
            control.close();
            return false;
        }
        accepted.controls.retain(|weak| {
            weak.upgrade()
                .is_some_and(|state| !Control(state).is_closed())
        });
        accepted.controls.push(Arc::downgrade(&control.0));
        true
    }

    /// Revoke all accepted transports, independently of per-Engine ownership.
    /// This also stops the listener from handing new peers to HTTP tasks.
    pub(crate) fn close(&self) {
        let controls = {
            let mut accepted = self.0.lock().unwrap_or_else(|error| error.into_inner());
            accepted.closed = true;
            std::mem::take(&mut accepted.controls)
        };
        for control in controls.into_iter().filter_map(|control| control.upgrade()) {
            Control(control).close();
        }
    }
}

/// An IO owner; only this value holds the admission permit.
/// The generic inner type supports deterministic in-memory IO tests.
pub(crate) struct ControlledStream<S = TcpStream> {
    stream: S,
    control: Control,
    _permit: OwnedSemaphorePermit,
}

impl<S> ControlledStream<S> {
    fn new(stream: S, permit: OwnedSemaphorePermit) -> Self {
        Self {
            stream,
            control: Control::default(),
            _permit: permit,
        }
    }
}

impl<S> Drop for ControlledStream<S> {
    fn drop(&mut self) {
        // Request/Engine.IO metadata can outlive this stream. A retained Control
        // must not make a disconnected TCP appear live or retain its permit.
        self.control.close();
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for ControlledStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Registration precedes the flag check so close cannot lose a wakeup.
        self.control.0.reader.register(cx.waker());
        if self.control.is_closed() {
            return Poll::Ready(Ok(()));
        }
        let filled = buffer.filled().len();
        let result = Pin::new(&mut self.stream).poll_read(cx, buffer);
        if self.control.is_closed() {
            buffer.set_filled(filled);
            Poll::Ready(Ok(()))
        } else {
            result
        }
    }
}

fn closed_write() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "notification transport was closed",
    )
}

impl<S: AsyncWrite + Unpin> AsyncWrite for ControlledStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.control.0.writer.register(cx.waker());
        if self.control.is_closed() {
            return Poll::Ready(Err(closed_write()));
        }
        let result = Pin::new(&mut self.stream).poll_write(cx, buffer);
        if self.control.is_closed() {
            Poll::Ready(Err(closed_write()))
        } else {
            result
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.control.0.writer.register(cx.waker());
        if self.control.is_closed() {
            return Poll::Ready(Err(closed_write()));
        }
        let result = Pin::new(&mut self.stream).poll_write_vectored(cx, buffers);
        if self.control.is_closed() {
            Poll::Ready(Err(closed_write()))
        } else {
            result
        }
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.control.0.writer.register(cx.waker());
        if self.control.is_closed() {
            return Poll::Ready(Err(closed_write()));
        }
        let result = Pin::new(&mut self.stream).poll_flush(cx);
        if self.control.is_closed() {
            Poll::Ready(Err(closed_write()))
        } else {
            result
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.control.0.writer.register(cx.waker());
        if self.control.is_closed() {
            // The owner is already terminating. It must not wait for an inner
            // writer that the remote peer deliberately stopped draining.
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

/// One global bound on accepted TCP owners, including health/idle HTTP peers.
pub(crate) struct ControlledListener {
    listener: TcpListener,
    permits: Arc<Semaphore>,
    shutdown: Shutdown,
}

impl ControlledListener {
    pub(crate) fn new(listener: TcpListener, max_tcp: usize) -> io::Result<Self> {
        if max_tcp == 0 || max_tcp > Semaphore::MAX_PERMITS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TCP connection limit is out of range",
            ));
        }
        Ok(Self {
            listener,
            permits: Arc::new(Semaphore::new(max_tcp)),
            shutdown: Shutdown::default(),
        })
    }

    pub(crate) fn shutdown_handle(&self) -> Shutdown {
        self.shutdown.clone()
    }
}

impl Listener for ControlledListener {
    type Io = ControlledStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            // Reuse Axum's retry/backoff for transient accept errors. Full
            // admission drops the raw socket before allocating control state.
            let (stream, address) = Listener::accept(&mut self.listener).await;
            if let Ok(permit) = self.permits.clone().try_acquire_owned() {
                let stream = ControlledStream::new(stream, permit);
                if self.shutdown.register(&stream.control) {
                    return (stream, address);
                }
                drop(stream);
                // The serve shutdown future will cancel this accept. Never
                // create new HTTP tasks after the registry's terminal close.
                return std::future::pending().await;
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

impl Connected<IncomingStream<'_, ControlledListener>> for Control {
    fn connect_info(stream: IncomingStream<'_, ControlledListener>) -> Self {
        stream.io().control.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::AtomicUsize,
        task::{Wake, Waker},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Default)]
    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn stream<S>(inner: S) -> (ControlledStream<S>, Arc<Semaphore>) {
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits.clone().try_acquire_owned().unwrap();
        (ControlledStream::new(inner, permit), permits)
    }

    #[tokio::test]
    async fn normal_io_delegates_until_group_revocation() {
        let (inner, mut peer) = tokio::io::duplex(16);
        let (mut stream, _) = stream(inner);
        stream.write_all(b"hello").await.unwrap();
        let mut bytes = [0; 5];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"hello");
        peer.write_all(b"world").await.unwrap();
        stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"world");
        let group = Group::default();
        group.register(&stream.control);
        group.close();
        assert_eq!(stream.read(&mut bytes).await.unwrap(), 0);
        assert_eq!(
            stream.write(b"blocked").await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[tokio::test]
    async fn close_wakes_both_pending_directions_and_does_not_lose_early_close() {
        let (inner, _peer) = tokio::io::duplex(1);
        let (mut stream, _) = stream(inner);
        stream.write_all(b"x").await.unwrap();
        let reader = Arc::new(Wakes::default());
        let writer = Arc::new(Wakes::default());
        let reader_waker = Waker::from(reader.clone());
        let writer_waker = Waker::from(writer.clone());
        let mut read_context = Context::from_waker(&reader_waker);
        let mut write_context = Context::from_waker(&writer_waker);
        let mut bytes = [0; 1];
        let mut buffer = ReadBuf::new(&mut bytes);
        assert!(Pin::new(&mut stream)
            .poll_read(&mut read_context, &mut buffer)
            .is_pending());
        assert!(Pin::new(&mut stream)
            .poll_write(&mut write_context, b"y")
            .is_pending());
        stream.control.close();
        assert_eq!(reader.0.load(Ordering::SeqCst), 1);
        assert_eq!(writer.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            Pin::new(&mut stream).poll_read(&mut read_context, &mut buffer),
            Poll::Ready(Ok(()))
        ));
        assert!(buffer.filled().is_empty());
        assert!(
            matches!(Pin::new(&mut stream).poll_write(&mut write_context, b"y"), Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::BrokenPipe)
        );
        assert!(
            matches!(Pin::new(&mut stream).poll_flush(&mut write_context), Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::BrokenPipe)
        );
        assert!(matches!(
            Pin::new(&mut stream).poll_shutdown(&mut write_context),
            Poll::Ready(Ok(()))
        ));
        // Another close after waker consumption is harmless; future polls see
        // the flag even when no new wake is emitted.
        stream.control.close();
        assert_eq!(stream.read(&mut [0; 1]).await.unwrap(), 0);
    }

    #[test]
    fn membership_is_weak_deduplicated_pruned_and_closed_for_future_members() {
        let group = Group::default();
        let first = Control::default();
        for _ in 0..1000 {
            group.register(&first);
        }
        assert_eq!(group.0.lock().unwrap().controls.len(), 1);
        drop(first);
        let second = Control::default();
        group.register(&second);
        assert_eq!(group.0.lock().unwrap().controls.len(), 1);
        let third = Control::default();
        group.register(&third);
        second.close();
        group.register(&third);
        assert_eq!(group.0.lock().unwrap().controls.len(), 1);
        group.close();
        assert!(third.is_closed());
        assert!(group.0.lock().unwrap().controls.is_empty());
        let late = Control::default();
        group.register(&late);
        assert!(
            late.is_closed(),
            "new polling/upgrade TCP cannot revive a retired session"
        );
    }

    #[test]
    fn retained_request_metadata_does_not_retain_a_dropped_tcp_permit() {
        let (inner, _peer) = tokio::io::duplex(1);
        let (stream, permits) = stream(inner);
        let metadata = stream.control.clone();
        let group = Group::default();
        group.register(&metadata);
        assert_eq!(permits.available_permits(), 0);
        drop(stream);
        assert!(metadata.is_closed());
        assert_eq!(permits.available_permits(), 1);
        let another = Control::default();
        group.register(&another);
        assert_eq!(group.0.lock().unwrap().controls.len(), 1);
    }

    #[tokio::test]
    async fn keepalive_reassignment_removes_the_previous_engines_kill_right() {
        let (inner, mut peer) = tokio::io::duplex(16);
        let (mut stream, _) = stream(inner);
        let old = Group::default();
        let current = Group::default();
        assert_ne!(old.0.lock().unwrap().id, current.0.lock().unwrap().id);
        old.register(&stream.control);
        current.register(&stream.control);
        old.close();
        assert!(!stream.control.is_closed());
        stream.write_all(b"current engine").await.unwrap();
        let mut bytes = [0; 14];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"current engine");
        current.close();
        assert_eq!(stream.read(&mut bytes).await.unwrap(), 0);
        assert_eq!(
            stream.write(b"denied").await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[tokio::test]
    async fn a_health_request_detaches_old_ownership_without_reopening_revoked_io() {
        let (inner, mut peer) = tokio::io::duplex(16);
        let (mut stream, _) = stream(inner);
        let old = Group::default();
        old.register(&stream.control);
        stream.control.detach();
        old.close();
        assert!(!stream.control.is_closed());
        peer.write_all(b"health").await.unwrap();
        let mut bytes = [0; 6];
        stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"health");

        let current = Group::default();
        current.register(&stream.control);
        current.close();
        stream.control.detach();
        let replacement = Group::default();
        replacement.register(&stream.control);
        assert!(stream.control.is_closed());
        assert_eq!(stream.read(&mut bytes).await.unwrap(), 0);
    }

    #[test]
    fn a_successful_revocation_claim_cannot_be_overwritten_by_a_later_owner() {
        let control = Control::default();
        let group = Group::default();
        group.register(&control);
        let id = group.0.lock().unwrap().id;
        // Exercise the state between owner CAS and the subsequent bool/wakeup.
        control
            .0
            .owner
            .compare_exchange(id, CLOSED_OWNER, Ordering::AcqRel, Ordering::Acquire)
            .unwrap();
        assert!(!control.0.closed.load(Ordering::Acquire));
        assert!(control.is_closed());
        control.detach();
        assert!(!control.assign_owner(id + 1));
        assert_eq!(control.0.owner.load(Ordering::Acquire), CLOSED_OWNER);
        control.close();
        assert!(control.0.closed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn global_shutdown_wakes_idle_and_partial_http_before_any_namespace_exists() {
        let permits = Arc::new(Semaphore::new(2));
        let (idle, _idle_peer) = tokio::io::duplex(128);
        let (partial, mut partial_peer) = tokio::io::duplex(128);
        let mut idle = ControlledStream::new(idle, permits.clone().try_acquire_owned().unwrap());
        let mut partial =
            ControlledStream::new(partial, permits.clone().try_acquire_owned().unwrap());
        let shutdown = Shutdown::default();
        assert!(shutdown.register(&idle.control));
        assert!(shutdown.register(&partial.control));
        // These peers have no current Engine owner; a per-Engine Group cannot
        // close them. This is the state of the real incomplete-head regression.
        idle.control.detach();
        partial.control.detach();
        let incomplete = b"GET /health/live HTTP/1.1\r\nX-Incomplete:";
        partial_peer.write_all(incomplete).await.unwrap();
        let wakes = Arc::new(Wakes::default());
        let waker = Waker::from(wakes.clone());
        let mut context = Context::from_waker(&waker);
        let mut bytes = [0; 128];
        let mut buffer = ReadBuf::new(&mut bytes);
        assert!(matches!(
            Pin::new(&mut partial).poll_read(&mut context, &mut buffer),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(buffer.filled(), incomplete);
        buffer.clear();
        assert!(Pin::new(&mut partial)
            .poll_read(&mut context, &mut buffer)
            .is_pending());
        assert!(Pin::new(&mut idle)
            .poll_read(&mut context, &mut buffer)
            .is_pending());
        shutdown.close();
        assert_eq!(wakes.0.load(Ordering::SeqCst), 2);
        assert_eq!(idle.read(&mut bytes).await.unwrap(), 0);
        assert_eq!(partial.read(&mut bytes).await.unwrap(), 0);
        assert_eq!(
            partial.write(b"blocked").await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(permits.available_permits(), 0);
        drop(idle);
        drop(partial);
        assert_eq!(permits.available_permits(), 2);
        assert!(shutdown.0.lock().unwrap().controls.is_empty());
        let late = Control::default();
        assert!(!shutdown.register(&late));
        assert!(late.is_closed());
    }

    #[test]
    fn global_registry_prunes_closed_controls_even_when_request_metadata_is_retained() {
        let shutdown = Shutdown::default();
        let mut retained_metadata = Vec::new();
        for _ in 0..1000 {
            let control = Control::default();
            assert!(shutdown.register(&control));
            assert_eq!(shutdown.0.lock().unwrap().controls.len(), 1);
            control.close();
            retained_metadata.push(control);
        }
        shutdown.close();
        assert!(shutdown.0.lock().unwrap().controls.is_empty());
        assert!(retained_metadata.iter().all(Control::is_closed));
    }
}
