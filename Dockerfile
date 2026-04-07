FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY src/ ./src/

RUN pip install uv && uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

CMD ["stock-mcp", "--http"]
