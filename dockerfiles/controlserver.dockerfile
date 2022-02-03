# syntax=docker/dockerfile:1
FROM --platform=linux/amd64 python:3.9.10-bullseye AS builder
RUN pip3 install poetry
WORKDIR /app
COPY pyconsole ./pyconsole
WORKDIR /app/pyconsole
RUN poetry config virtualenvs.create false
RUN apt-get install openssh-client
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
RUN --mount=type=ssh poetry update && poetry install --no-dev --no-root #-vvv
ENV PYTHONPATH=/app/pyconsole/gen:/app/pyconsole:${PYTHONPATH}
CMD ["poetry", "run", "python", "app.py"]
