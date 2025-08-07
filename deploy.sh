#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status

echo "Starting deployment..."

# Create the deployment directory if it doesn't exist
mkdir -p /root/skylance
cd /root/skylance

echo "Pulling latest Docker image..."
docker pull yourdockerhubusername/skylance-ml:latest

echo "Stopping existing container (if any)..."
docker stop skylance || true

echo "Removing existing container (if any)..."
docker rm skylance || true

echo "Starting new container..."
docker run --env-file /root/.env -d --name skylance -p 80:8000 yourdockerhubusername/skylance-ml:latest

echo "Deployment completed!"