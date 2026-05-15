FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY config.example.yaml ./config.yaml

RUN pip install --no-cache-dir .

CMD ["oi-screener", "run", "--config", "config.yaml"]
