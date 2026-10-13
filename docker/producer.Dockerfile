FROM python:3.11-slim-bookworm
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka==2.5.0 pandas==2.2.2 pyarrow==16.1.0
COPY producer ./producer
ENTRYPOINT ["python", "-m", "producer.producer"]
