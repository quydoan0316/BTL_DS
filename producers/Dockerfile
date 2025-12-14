FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer code
COPY . .

# Default command (can be overridden in docker-compose)
CMD ["python", "region_producer.py", "--region", "AN"]
