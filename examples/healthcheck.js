#!/usr/bin/env node

const fs = require('node:fs')
const tlsEnabled = Boolean(process.env.YASD_TLS_KEY || process.env.YASD_TLS_CERT)
const protocol = require(tlsEnabled ? 'node:https' : 'node:http')

const port = Number(process.env.YASD_PORT || 7379)
if (!Number.isInteger(port) || port < 1 || port > 65535) process.exit(1)

const options = {
  host: process.env.YASD_HEALTHCHECK_HOST || '127.0.0.1',
  port,
  path: '/readyz',
}

if (tlsEnabled) {
  // Never disable verification. Public-CA certificates use Node's default
  // trust store; private-CA deployments must provide the CA explicitly.
  options.rejectUnauthorized = true
  options.servername = process.env.YASD_HEALTHCHECK_SERVERNAME || options.host
  const caPath = process.env.YASD_HEALTHCHECK_CA
  if (caPath) options.ca = fs.readFileSync(caPath)

  const clientCertPath = process.env.YASD_HEALTHCHECK_CLIENT_CERT
  const clientKeyPath = process.env.YASD_HEALTHCHECK_CLIENT_KEY
  if (Boolean(clientCertPath) !== Boolean(clientKeyPath)) process.exit(1)
  if (clientCertPath && clientKeyPath) {
    options.cert = fs.readFileSync(clientCertPath)
    options.key = fs.readFileSync(clientKeyPath)
  }
}

const request = protocol.get({
  ...options,
}, response => {
  const healthy = response.statusCode === 200
  response.resume()
  response.on('end', () => process.exit(healthy ? 0 : 1))
})

request.setTimeout(2500, () => request.destroy())
request.on('error', () => process.exit(1))
