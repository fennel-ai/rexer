# syntax=docker/dockerfile:1
FROM --platform=linux/amd64 python:3.9.10-bullseye AS builder
RUN pip3 install poetry
WORKDIR /app
COPY pyclient ./pyclient
WORKDIR /app/pyclient
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev --no-root
ENV PYTHONPATH=/app/pyclient/gen:/app/pyclient:${PYTHONPATH}
CMD ["poetry", "run", "python", "app.py"]
