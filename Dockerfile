FROM node:24.3.0-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts --no-audit --no-fund
COPY index.ts config.mjs platform-session.mjs postgres-store.mjs service.mjs observability.mjs ./
ENV NODE_ENV=production NOTIFICATION_PORT=3003 NOTIFICATION_HOST=0.0.0.0 NOTIFICATION_SOCKET_PATH=/socket.io/
USER node
EXPOSE 3003
HEALTHCHECK --interval=30s --timeout=4s --start-period=20s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+process.env.NOTIFICATION_PORT+'/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
CMD ["node", "index.ts"]
