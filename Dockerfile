FROM python:3.12-slim

WORKDIR /app

COPY api/requirements.txt /app/api/requirements.txt
COPY sync/requirements.txt /app/sync/requirements.txt

RUN pip install --no-cache-dir \
    -r /app/api/requirements.txt \
    -r /app/sync/requirements.txt

COPY api/      /app/api/
COPY sync/     /app/sync/
COPY frontend/ /app/frontend/

WORKDIR /app

EXPOSE 8000

CMD ["sh", "-c", "uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
