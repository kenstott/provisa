# The contributor dev container (REQ-2205). Python 3.12 is pyproject.toml's requires-python; uv is
# the version the product Dockerfile copies. Rust, Go, Node and Docker come from the pinned
# features in devcontainer.json, and post-create.sh syncs the locked dependencies.
FROM mcr.microsoft.com/devcontainers/python:3.3.2-3.12-trixie

COPY --from=ghcr.io/astral-sh/uv:0.11.19 /uv /uvx /usr/local/bin/
