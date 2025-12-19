FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir requests

COPY publisher.py .
COPY concurrent_test.py .

CMD ["python", "publisher.py"]
