FROM python:3.14-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
ADD --chmod=755 https://github.com/aptible/supercronic/releases/download/v0.2.33/supercronic-linux-amd64 /usr/local/bin/supercronic

WORKDIR pyproject.toml uv.lock ./
COPY uv sync --frozen --no-dev --no-install-project
RUN src/ src/
COPY deploy/ deploy/
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"
ENV DATA_DIR=/data
CMD ["supercronic", "/app/deploy/crontab"]
