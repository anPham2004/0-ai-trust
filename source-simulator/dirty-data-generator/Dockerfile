FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dirty_data.py .
USER 65532:65532
ENTRYPOINT ["python", "dirty_data.py"]
