#!/usr/bin/env node
// `yasd-server` — standalone YASD cache server for multi-instance Echo.
// Config via flags or env (flags win):
//   --port 7379            YASD_PORT
//   --host 0.0.0.0         YASD_HOST
//   --snapshot /data/s.json YASD_SNAPSHOT
//   --aof /data/a.aof      YASD_AOF
//   --auto-save-ms 60000   YASD_AUTO_SAVE_MS
//   --no-load              YASD_LOAD_ON_START=0
//   --no-save-on-shutdown  YASD_SAVE_ON_SHUTDOWN=0
//   --max-entries 10000    CACHE_MAX_ENTRIES
//   --max-bytes 67108864   CACHE_MAX_BYTES
//   --default-ttl-ms 15000 CACHE_DEFAULT_TTL_MS
//   --password s3cret      YASD_PASSWORD (AUTH required)
//   --tls-key key.pem      YASD_TLS_KEY (PEM path; needs --tls-cert)
//   --tls-cert cert.pem    YASD_TLS_CERT (PEM path; needs --tls-key)

import * as fs from 'fs';
import { YasdServer, serverOptionsFromEnv } from './server';

function parseArgv(argv: string[]): Record<string, string | boolean> {
  const out: Record<string, string | boolean> = {};
  for (let i = 0; i < argv.length; i++) {
    const raw = argv[i] as string;
    if (!raw.startsWith('--')) continue;
    const eq = raw.indexOf('=');
    if (eq !== -1) {
      out[raw.slice(2, eq)] = raw.slice(eq + 1);
      continue;
    }
    const key = raw.slice(2);
    if (key.startsWith('no-')) {
      out[key] = false;
      continue;
    }
    const next = argv[i + 1];
    if (next !== undefined && !next.startsWith('--')) {
      out[key] = next;
      i++;
    } else {
      out[key] = true;
    }
  }
  return out;
}

async function main(): Promise<void> {
  const args = parseArgv(process.argv.slice(2));
  if (args.help === true || args.h === true) {
    console.log('Usage: yasd-server [options]');
    console.log('  --port, --host, --snapshot, --aof, --auto-save-ms,');
    console.log('  --no-load, --no-save-on-shutdown,');
    console.log('  --max-entries, --max-bytes, --default-ttl-ms,');
    console.log('  --password, --tls-key <pem>, --tls-cert <pem>');
    console.log('Env: YASD_PORT YASD_HOST YASD_SNAPSHOT YASD_AOF YASD_AUTO_SAVE_MS');
    console.log('     CACHE_MAX_ENTRIES CACHE_MAX_BYTES CACHE_DEFAULT_TTL_MS CACHE_NAMESPACE_TTLS');
    console.log('     YASD_PASSWORD YASD_TLS_KEY YASD_TLS_CERT');
    return;
  }

  const base = serverOptionsFromEnv(process.env);
  if (typeof args.port === 'string') base.port = parseInt(args.port, 10);
  if (typeof args.host === 'string') base.host = args.host;
  if (typeof args.snapshot === 'string') base.snapshotPath = args.snapshot;
  if (typeof args.aof === 'string') base.aofPath = args.aof;
  if (typeof args['auto-save-ms'] === 'string') base.autoSaveMs = parseInt(args['auto-save-ms'], 10);
  if (args.load === false) base.loadOnStart = false;
  if (args['save-on-shutdown'] === false) base.saveOnShutdown = false;
  base.cache = base.cache ?? {};
  if (typeof args['max-entries'] === 'string') base.cache.maxEntries = parseInt(args['max-entries'], 10);
  if (typeof args['max-bytes'] === 'string') base.cache.maxBytes = parseInt(args['max-bytes'], 10);
  if (typeof args['default-ttl-ms'] === 'string') {
    base.cache.defaultTTLMs = parseInt(args['default-ttl-ms'], 10);
  }
  if (typeof args.password === 'string') base.password = args.password;
  if (typeof args.requirepass === 'string') base.password = args.requirepass;
  const tlsKey = args['tls-key'];
  const tlsCert = args['tls-cert'];
  if (tlsKey !== undefined || tlsCert !== undefined) {
    if (typeof tlsKey !== 'string' || typeof tlsCert !== 'string') {
      throw new Error('--tls-key and --tls-cert must both be PEM file paths');
    }
    base.tls = { key: fs.readFileSync(tlsKey, 'utf8'), cert: fs.readFileSync(tlsCert, 'utf8') };
  }

  const server = new YasdServer(base);
  const shutdown = (signal: string): void => {
    console.log(`yasd: ${signal}, shutting down...`);
    server
      .close()
      .then(() => process.exit(0))
      .catch(err => {
        console.error(`yasd: shutdown error: ${(err as Error).message}`);
        process.exit(1);
      });
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  await server.start();
  const addr = server.address();
  console.log(`yasd: listening on ${addr.host}:${addr.port} (health: http://${addr.host}:${addr.port}/healthz)`);
  if (base.snapshotPath) console.log(`yasd: snapshot: ${base.snapshotPath}`);
  if (base.aofPath) console.log(`yasd: aof: ${base.aofPath}`);
}

main().catch(err => {
  console.error(`yasd: failed to start: ${(err as Error).message}`);
  process.exit(1);
});
