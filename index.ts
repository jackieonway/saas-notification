// Node 24 runs this entry directly; no JWT signing key is loaded.
import { runFromEnvironment } from './service.mjs'

await runFromEnvironment()
