# YASD cache server (multi-instance Echo). Build context is the repo root.
FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev 2>/dev/null || npm install --omit=dev

COPY dist ./dist

EXPOSE 7379
VOLUME ["/data"]

ENV YASD_PORT=7379 \
    YASD_HOST=0.0.0.0 \
    YASD_SNAPSHOT=/data/snapshot.json \
    YASD_AOF=/data/appendonly.aof \
    YASD_AUTO_SAVE_MS=60000

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:7379/healthz').then(r=>{if(!r.ok)process.exit(1)}).catch(()=>process.exit(1))"

CMD ["node", "dist/cli.js"]
