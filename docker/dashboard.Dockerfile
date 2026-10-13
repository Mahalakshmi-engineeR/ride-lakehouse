FROM python:3.11-slim-bookworm
RUN mkdir -p /data && chmod 777 /data
WORKDIR /app
RUN pip install --no-cache-dir streamlit==1.37.1 duckdb==1.1.3 pandas plotly
COPY dashboard/app.py ./app.py
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
