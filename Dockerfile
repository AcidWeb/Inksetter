FROM python:3.14-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
COPY pyproject.toml uv.lock ./
COPY inksetter ./inksetter
RUN uv sync --frozen --no-install-project --no-dev \
 && rm -rf /root/.cache/uv

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

VOLUME ["/cache"]
EXPOSE 8080

CMD ["uvicorn", "inksetter.app:app", "--host", "0.0.0.0", "--port", "8080"]
