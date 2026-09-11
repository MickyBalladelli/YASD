# syntax=docker/dockerfile:1.7
# YASD cache server (multi-instance Echo). Build context is the repo root.
ARG NODE_VERSION=20-alpine

FROM node:${NODE_VERSION} AS build

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

FROM node:${NODE_VERSION} AS runtime

ENV NODE_ENV=production
WORKDIR /app

RUN addgroup -S yasd \
  && adduser -S -G yasd yasd \
  && mkdir -p /data \
  && chown yasd:yasd /data

COPY --from=build --chown=yasd:yasd /app/dist ./dist
COPY --chown=yasd:yasd package.json examples/healthcheck.js ./

EXPOSE 7379
VOLUME ["/data"]

USER yasd
STOPSIGNAL SIGTERM

ENV YASD_PORT=7379 \
    YASD_HOST=0.0.0.0 \
    YASD_SNAPSHOT=/data/snapshot.json \
    YASD_AOF=/data/appendonly.aof \
    YASD_AUTO_SAVE_MS=60000

# Optional security settings:
# YASD_PASSWORD, YASD_HEALTH_DETAILS, YASD_HEALTH_TOKEN,
# YASD_TLS_KEY, YASD_TLS_CERT, YASD_TLS_CA,
# YASD_TLS_REQUEST_CERT, YASD_TLS_REJECT_UNAUTHORIZED
# YASD_HEALTHCHECK_CA, YASD_HEALTHCHECK_SERVERNAME,
# YASD_HEALTHCHECK_CLIENT_CERT, YASD_HEALTHCHECK_CLIENT_KEY

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD ["node", "healthcheck.js"]

CMD ["node", "dist/cli.js"]
