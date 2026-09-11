#!/usr/bin/env node

const protocol = require(process.env.YASD_TLS_KEY ? 'node:https' : 'node:http')

const request = protocol.get({
  host: '127.0.0.1',
  port: Number(process.env.YASD_PORT || 7379),
  path: '/readyz',
  rejectUnauthorized: false
}, response => {
  const healthy = response.statusCode === 200
  response.resume()
  response.on('end', () => process.exit(healthy ? 0 : 1))
})

request.setTimeout(2500, () => request.destroy())
request.on('error', () => process.exit(1))
