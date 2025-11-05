# syntax=docker/dockerfile:1

FROM python:3.11-slim

# Ensure stdout/stderr are unbuffered and no .pyc files are written
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install system dependencies if needed (uncomment if required)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (better build caching)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project
COPY . .

# Expose Flask port
EXPOSE 5000

# Default command runs the Flask app directly to ensure startup routines execute
CMD ["python", "main.py"]
