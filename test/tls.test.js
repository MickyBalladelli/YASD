const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { execFileSync } = require("node:child_process");
const { YasdClient, YasdServer } = require("../dist");
const { resolveCliOptions } = require("../dist/cli");

test("real TLS/mTLS: verified health and commands, invalid trust/identity/client certificate rejected", async (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "yasd-tls-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const keyPath = path.join(dir, "key.pem");
  const certPath = path.join(dir, "cert.pem");
  execFileSync(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-days",
      "1",
      "-subj",
      "/CN=localhost",
      "-addext",
      "subjectAltName=DNS:localhost,IP:127.0.0.1",
      "-keyout",
      keyPath,
      "-out",
      certPath,
    ],
    { stdio: "ignore", timeout: 30_000 },
  );
  const key = fs.readFileSync(keyPath);
  const cert = fs.readFileSync(certPath);
  const options = resolveCliOptions(
    ["--tls-min-version", "TLSv1.3", "--tls-request-cert"],
    {
      YASD_TLS_KEY: keyPath,
      YASD_TLS_CERT: certPath,
      YASD_TLS_CA: certPath,
    },
  );
  assert.equal(options.tls.minVersion, "TLSv1.3");
  assert.equal(options.tls.requestCert, true);
  const server = new YasdServer({
    port: 0,
    password: "test-only",
    tls: {
      key,
      cert,
      ca: cert,
      requestCert: true,
      rejectUnauthorized: true,
      minVersion: "TLSv1.3",
    },
  });
  await server.start();
  const clients = [];
  t.after(async () => {
    await Promise.all(clients.map((client) => client.close()));
    await server.close();
  });
  const connect = (tls) => {
    const client = new YasdClient({
      host: "127.0.0.1",
      port: server.address().port,
      password: "test-only",
      poolSize: 1,
      connectTimeoutMs: 1000,
      requestTimeoutMs: 1000,
      tls,
    });
    clients.push(client);
    return client;
  };
  const good = connect({ ca: cert, key, cert, servername: "localhost" });
  assert.equal(await good.ping(), "PONG");
  assert.equal((await good.healthcheck()).status, "ok");
  await good.set("tls", { verified: true });
  assert.deepEqual(await good.get("tls"), { verified: true });
  await assert.rejects(connect({ key, cert }).ping());
  await assert.rejects(
    connect({ ca: cert, key, cert, servername: "wrong.invalid" }).ping(),
  );
  await assert.rejects(connect({ ca: cert }).ping());
});
