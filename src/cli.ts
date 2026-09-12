#!/usr/bin/env node
import { YasdServer, YasdServerOptions, serverOptionsFromEnv } from './server';

const flags: Record<string, string> = {
  port: 'YASD_PORT', host: 'YASD_HOST', snapshot: 'YASD_SNAPSHOT', aof: 'YASD_AOF',
  'auto-save-ms': 'YASD_AUTO_SAVE_MS', load: 'YASD_LOAD_ON_START',
  'save-on-shutdown': 'YASD_SAVE_ON_SHUTDOWN', 'max-entries': 'CACHE_MAX_ENTRIES',
  'max-bytes': 'CACHE_MAX_BYTES', 'max-key-bytes': 'CACHE_MAX_KEY_BYTES',
  'max-value-bytes': 'CACHE_MAX_VALUE_BYTES', 'default-ttl-ms': 'CACHE_DEFAULT_TTL_MS',
  'namespace-ttls': 'CACHE_NAMESPACE_TTLS', 'slow-command-ms': 'YASD_SLOW_COMMAND_MS',
  'max-pending-output-bytes': 'YASD_MAX_PENDING_OUTPUT_BYTES', 'max-command-ms': 'YASD_MAX_COMMAND_MS',
  'idle-connection-timeout-ms': 'YASD_IDLE_CONNECTION_TIMEOUT_MS', 'shutdown-deadline-ms': 'YASD_SHUTDOWN_DEADLINE_MS',
  'health-details': 'YASD_HEALTH_DETAILS', 'health-token': 'YASD_HEALTH_TOKEN',
  password: 'YASD_PASSWORD', requirepass: 'YASD_PASSWORD', 'tls-key': 'YASD_TLS_KEY',
  'tls-cert': 'YASD_TLS_CERT', 'tls-ca': 'YASD_TLS_CA', 'tls-min-version': 'YASD_TLS_MIN_VERSION',
  'tls-request-cert': 'YASD_TLS_REQUEST_CERT', 'tls-reject-unauthorized': 'YASD_TLS_REJECT_UNAUTHORIZED',
  'max-inflight-bytes': 'YASD_MAX_INFLIGHT_BYTES',
  'max-connections': 'YASD_MAX_CONNECTIONS', 'max-queued-requests': 'YASD_MAX_QUEUED_REQUESTS',
  'max-transaction-commands': 'YASD_MAX_TRANSACTION_COMMANDS', 'max-transaction-bytes': 'YASD_MAX_TRANSACTION_BYTES',
  'max-watched-keys': 'YASD_MAX_WATCHED_KEYS', 'max-subscriptions': 'YASD_MAX_SUBSCRIPTIONS',
};
const booleans = new Set(['load', 'save-on-shutdown', 'health-details', 'tls-request-cert', 'tls-reject-unauthorized']);

/** Merge flags before parsing: a valid flag can override an invalid environment value. */
export function resolveCliOptions(argv: string[], env: NodeJS.ProcessEnv = process.env): YasdServerOptions {
  const merged = { ...env };
  for (let i = 0; i < argv.length; i++) {
    const match = /^--([^=]+)(?:=(.*))?$/.exec(argv[i]);
    if (!match) throw new Error(`unexpected argument: ${argv[i]}`);
    let name = match[1];
    const negated = name.startsWith('no-');
    if (negated) name = name.slice(3);
    if (!Object.prototype.hasOwnProperty.call(flags, name) || (negated && !booleans.has(name))) {
      throw new Error(`unknown option: --${match[1]}`);
    }
    let value = match[2];
    if (negated) {
      if (value !== undefined) throw new Error(`--no-${name} takes no value`);
      value = 'false';
    } else if (value === undefined) {
      if (booleans.has(name)) {
        const next = argv[i + 1];
        if (next && /^(true|false|0|1)$/i.test(next)) { value = next; i++; }
        else value = 'true';
      } else {
        value = argv[++i];
        if (value === undefined || value.startsWith('--')) throw new Error(`--${name} requires a value`);
      }
    }
    merged[flags[name]] = value;
  }
  return serverOptionsFromEnv(merged);
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  if (argv.includes('--help') || argv.includes('-h') || argv.includes('--h')) {
    console.log('Usage: yasd-server [options]\nBoolean options also accept --no-<option>.');
    for (const [flag, env] of Object.entries(flags)) console.log(`  --${flag}${booleans.has(flag) ? '[=true|false]' : ' <value>'}  ${env}`);
    return;
  }
  const server = new YasdServer(resolveCliOptions(argv));
  let stopping: Promise<void> | undefined;
  const shutdown = (): void => {
    if (stopping) return;
    stopping = server.close().then(() => { process.exitCode = 0; }, error => {
      console.error(`yasd: shutdown error: ${error.message}`);
      process.exitCode = 1;
    });
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
  try {
    await server.start();
    const addr = server.address();
    console.log(`yasd: listening on ${addr.host}:${addr.port} (ready: ${server.tlsEnabled ? 'https' : 'http'}://${addr.host}:${addr.port}/readyz)`);
  } catch (error) {
    await server.close().catch(() => undefined);
    throw error;
  }
}

if (require.main === module) main().catch(error => {
  console.error(`yasd: failed to start: ${error.message}`);
  process.exitCode = 1;
});
