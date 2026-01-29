FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies directly
RUN pip install --no-cache-dir \
    kafka-python>=2.0.2 \
    python-dotenv>=1.1.1 \
    psycopg2-binary>=2.9.9

# Copy application code
COPY scripts/ ./scripts/
COPY secrets/ ./secrets/

# Set Python path
ENV PYTHONPATH=/app

# Run the Kafka consumer
CMD ["python", "scripts/kafka/kafka_consumer.py"]

