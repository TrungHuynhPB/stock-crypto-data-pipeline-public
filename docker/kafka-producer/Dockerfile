FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies directly
RUN pip install --no-cache-dir \
    kafka-python>=2.0.2 \
    faker>=37.6.0 \
    pandas>=2.3.2 \
    python-dotenv>=1.1.1

# Copy application code
COPY scripts/ ./scripts/
COPY seeds/ ./seeds/

# Set Python path
ENV PYTHONPATH=/app

# Run the Kafka producer
CMD ["python", "scripts/kafka/kafka_producer.py"]

