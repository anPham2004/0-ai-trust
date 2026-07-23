FROM python:3.12-alpine

WORKDIR /app
COPY requirements.txt register.py ./
RUN pip install --no-cache-dir -r requirements.txt

USER 65532:65532
ENTRYPOINT ["python", "register.py"]
