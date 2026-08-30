# Cloud Run container for the game-event simulator.
#
# Build from the REPO ROOT so src/ and config/ are in context:
#   docker build -f simulator/Dockerfile -t chess-simulator .
#
# Deploy (concurrency 1 — see the note in main.py):
#   gcloud run deploy chess-simulator --source . \
#     --set-env-vars CHESS_STREAMING__PUBLISHER=pubsub,CHESS_PROJECT__GCP_PROJECT_ID=$PROJECT \
#     --max-instances 1 --concurrency 1 --timeout 3600

FROM python:3.12-slim

# Fail fast and keep logs unbuffered so Cloud Logging sees output immediately.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    CHESS_LOG_JSON=true \
    PYTHONPATH=/app/src

WORKDIR /app

# Only the deps the simulator actually needs — no BigQuery, no Beam. A slimmer
# image is a smaller attack surface and a faster cold start.
COPY simulator/requirements.txt ./simulator/requirements.txt
RUN pip install -r simulator/requirements.txt

COPY src/ ./src/
COPY config/ ./config/
COPY simulator/main.py ./simulator/main.py

# Run as a non-root user; Cloud Run does not require root and nothing here needs it.
RUN useradd --create-home --uid 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8080
CMD ["python", "-m", "simulator.main"]
