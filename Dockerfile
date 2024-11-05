FROM python:3.12.1-slim-bullseye as builder

COPY uv.lock pyproject.toml ./
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN uv pip compile pyproject.toml -o requirements.txt

FROM python:3.12.1-slim-bullseye as dev

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY --from=builder requirements.txt /app

RUN apt update -y && \
    apt install -y python3-dev \
    gcc \
    musl-dev && \
    pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY /app/ /app/**

EXPOSE 8000