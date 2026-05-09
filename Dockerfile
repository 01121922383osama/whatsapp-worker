FROM node:22-alpine
WORKDIR /worker
ENV NODE_ENV=production
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY . .
CMD ["npm", "run", "start"]
