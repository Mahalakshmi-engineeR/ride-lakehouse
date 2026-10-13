FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
RUN pip install --no-cache-dir pyspark==3.5.1 delta-spark==3.2.0 deltalake==0.20.2
COPY streaming ./streaming
# Warm the Ivy cache so the Kafka/Delta/S3A jars are baked into the image
RUN python -c "from streaming.spark_session import get_spark; get_spark('warmup').stop()" || echo "warmup skipped"
CMD ["python", "-m", "streaming.bronze_job"]
