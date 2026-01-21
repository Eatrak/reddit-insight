FROM node:22-alpine

WORKDIR /app

ENV NODE_ENV=production

COPY package*.json tsconfig.json ./ 

RUN npm install --include=dev

COPY src ./src

RUN npx tsc && npm prune --production

EXPOSE 8000

CMD ["node", "dist/index.js"]

