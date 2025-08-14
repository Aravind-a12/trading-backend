FROM python:3.11-slim

# Prevent interactive prompts during apt install
ENV DEBIAN_FRONTEND=noninteractive

# Install system deps for psycopg2 and cryptofeed optional libs
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       libpq-dev \
       curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Env defaults (can be overridden at deploy time)
ENV PORT=8080 \
    REDIS_HOST=127.0.0.1 \
    REDIS_PORT=6379 \
    PYTHONUNBUFFERED=1

# Expose for Cloud Run (honors $PORT)
EXPOSE 8080

# Start FastAPI app
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]


