# YASD cache server (multi-instance Echo). Build context is the repo root.
FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev 2>/dev/null || npm install --omit=dev

COPY dist ./dist
COPY examples/healthcheck.js ./healthcheck.js

EXPOSE 7379
VOLUME ["/data"]

ENV YASD_PORT=7379 \
    YASD_HOST=0.0.0.0 \
    YASD_SNAPSHOT=/data/snapshot.json \
    YASD_AOF=/data/appendonly.aof \
    YASD_AUTO_SAVE_MS=60000

# Optional security settings:
# YASD_PASSWORD, YASD_TLS_KEY, YASD_TLS_CERT, YASD_TLS_CA,
# YASD_TLS_REQUEST_CERT, YASD_TLS_REJECT_UNAUTHORIZED

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD node healthcheck.js

CMD ["node", "dist/cli.js"]
