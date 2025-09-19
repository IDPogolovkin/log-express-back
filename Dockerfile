# Use official Node.js LTS image
FROM node:20-alpine

# Set working directory
WORKDIR /app

# Copy package.json and package-lock.json first (to optimize caching)
COPY package.json package-lock.json ./

# Install dependencies (including Express)
RUN npm install

# Copy the rest of the application files
COPY . .

# Expose port 4000 for the API
EXPOSE 4000

# Start the API server
CMD ["node", "server.js"]