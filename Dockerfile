FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port 8080 (Cloud Run default)
EXPOSE 8080

# Run with Gunicorn + Uvicorn workers
# Increased timeout to 300s (5 mins) to allow for long sync operations
CMD ["gunicorn", "-w", "1", "-k", "uvicorn.workers.UvicornWorker", "main:app", "--bind", "0.0.0.0:8080", "--timeout", "300"]
