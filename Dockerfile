FROM node:20-slim

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

COPY tsconfig.json ./
COPY src/ ./src/

RUN npm run build

# Non-root user, read-only root filesystem
RUN chown -R node:node /app
USER node

# Workspace and config dirs will be mounted as emptyDir/PVC by k8s
# The process writes to STORE_DIR, GROUPS_DIR, DATA_DIR — all under /workspace
ENV NODE_ENV=production
ENV STORE_DIR=/workspace/store
ENV GROUPS_DIR=/workspace/groups
ENV DATA_DIR=/workspace/data

CMD ["node", "dist/index.js"]
