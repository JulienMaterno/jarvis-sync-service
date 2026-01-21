FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose ports (Cloud Run uses 8080, Docker bridge uses 8000)
EXPOSE 8080
EXPOSE 8000

# Run with Gunicorn + Uvicorn workers
# Increased timeout to 300s (5 mins) to allow for long sync operations
# Uses PORT env var if set, otherwise defaults to 8080 (Cloud Run)
CMD sh -c "gunicorn -w 1 -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:${PORT:-8080} --timeout 300"
