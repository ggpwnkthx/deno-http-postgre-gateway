# Stage 1: Get Deno binary
FROM denoland/deno:bin AS deno
# Stage 2: Main development environment
FROM mcr.microsoft.com/devcontainers/base:debian
# Copy Deno binary from Stage 1
COPY --from=deno /deno /usr/local/bin/deno