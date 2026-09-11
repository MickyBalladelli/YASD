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
//   --slow-command-ms 5    YASD_SLOW_COMMAND_MS (slow-command log threshold)
//   --max-pending-output-bytes 1048576 YASD_MAX_PENDING_OUTPUT_BYTES
//   --password s3cret      YASD_PASSWORD (AUTH required)
//   --tls-key key.pem      YASD_TLS_KEY (PEM path; needs --tls-cert)
//   --tls-cert cert.pem    YASD_TLS_CERT (PEM path; needs --tls-key)
//   --tls-ca ca.pem        YASD_TLS_CA (optional client CA)
//   --tls-min-version      YASD_TLS_MIN_VERSION (for example TLSv1.3)

import * as fs from 'fs';
import { YasdServer, serverOptionsFromEnv } from './server';
import {
  parsePort,
  parseStrictInteger,
  parseStrictNonNegativeNumber,
} from './validation';

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

function stringArg(args: Record<string, string | boolean>, name: string): string | undefined {
  const value = args[name];
  if (value === undefined) return undefined;
  if (typeof value !== 'string') throw new Error(`--${name} requires a value`);
  return value;
}

async function main(): Promise<void> {
  const args = parseArgv(process.argv.slice(2));
  if (args.help === true || args.h === true) {
    console.log('Usage: yasd-server [options]');
    console.log('  --port, --host, --snapshot, --aof, --auto-save-ms,');
    console.log('  --no-load, --no-save-on-shutdown,');
    console.log('  --max-entries, --max-bytes, --default-ttl-ms, --slow-command-ms,');
    console.log('  --max-pending-output-bytes,');
    console.log('  --password, --tls-key <pem>, --tls-cert <pem>, --tls-ca <pem>');
    console.log('Env: YASD_PORT YASD_HOST YASD_SNAPSHOT YASD_AOF YASD_AUTO_SAVE_MS YASD_SLOW_COMMAND_MS');
    console.log('     YASD_MAX_PENDING_OUTPUT_BYTES');
    console.log('     CACHE_MAX_ENTRIES CACHE_MAX_BYTES CACHE_DEFAULT_TTL_MS CACHE_NAMESPACE_TTLS');
    console.log('     YASD_PASSWORD YASD_TLS_KEY YASD_TLS_CERT YASD_TLS_CA YASD_TLS_MIN_VERSION');
    console.log('     YASD_TLS_REQUEST_CERT YASD_TLS_REJECT_UNAUTHORIZED');
    return;
  }

  const base = serverOptionsFromEnv(process.env);
  const port = stringArg(args, 'port');
  if (port !== undefined) base.port = parsePort(port, '--port');
  const host = stringArg(args, 'host');
  if (host !== undefined) base.host = host;
  const snapshot = stringArg(args, 'snapshot');
  if (snapshot !== undefined) base.snapshotPath = snapshot;
  const aof = stringArg(args, 'aof');
  if (aof !== undefined) base.aofPath = aof;
  const autoSaveMs = stringArg(args, 'auto-save-ms');
  if (autoSaveMs !== undefined) {
    base.autoSaveMs = parseStrictNonNegativeNumber(autoSaveMs, '--auto-save-ms');
  }
  if (args.load === false) base.loadOnStart = false;
  if (args['save-on-shutdown'] === false) base.saveOnShutdown = false;
  base.cache = base.cache ?? {};
  const maxEntries = stringArg(args, 'max-entries');
  if (maxEntries !== undefined) {
    base.cache.maxEntries = parseStrictInteger(maxEntries, '--max-entries', 1);
  }
  const maxBytes = stringArg(args, 'max-bytes');
  if (maxBytes !== undefined) {
    base.cache.maxBytes = parseStrictInteger(maxBytes, '--max-bytes', 1);
  }
  const defaultTtlMs = stringArg(args, 'default-ttl-ms');
  if (defaultTtlMs !== undefined) {
    base.cache.defaultTTLMs = parseStrictNonNegativeNumber(defaultTtlMs, '--default-ttl-ms');
  }
  const slowCommandMs = stringArg(args, 'slow-command-ms');
  if (slowCommandMs !== undefined) {
    base.slowCommandMs = parseStrictNonNegativeNumber(slowCommandMs, '--slow-command-ms');
  }
  const maxPendingOutputBytes = stringArg(args, 'max-pending-output-bytes');
  if (maxPendingOutputBytes !== undefined) {
    base.maxPendingOutputBytes = parseStrictInteger(
      maxPendingOutputBytes,
      '--max-pending-output-bytes',
      1
    );
  }
  const password = stringArg(args, 'password');
  if (password !== undefined) base.password = password;
  const requirepass = stringArg(args, 'requirepass');
  if (requirepass !== undefined) base.password = requirepass;
  const tlsKey = args['tls-key'];
  const tlsCert = args['tls-cert'];
  if (tlsKey !== undefined || tlsCert !== undefined) {
    if (typeof tlsKey !== 'string' || typeof tlsCert !== 'string') {
      throw new Error('--tls-key and --tls-cert must both be PEM file paths');
    }
    base.tls = { key: fs.readFileSync(tlsKey, 'utf8'), cert: fs.readFileSync(tlsCert, 'utf8') };
  }
  const tlsCa = args['tls-ca'];
  if (tlsCa !== undefined) {
    if (typeof tlsCa !== 'string') throw new Error('--tls-ca must be a PEM file path');
    if (!base.tls) throw new Error('--tls-ca requires TLS key and cert');
    base.tls.ca = fs.readFileSync(tlsCa, 'utf8');
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
