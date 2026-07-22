FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY exporter.py .
RUN mkdir -p /landing && chown -R 65532:65532 /landing
USER 65532:65532
CMD ["python", "exporter.py"]

