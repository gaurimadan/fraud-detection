FROM python:3.12-slim

WORKDIR /app
COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer/producer.py .
COPY render-model/ /models/
COPY tests/test_producer.py /app/tests/test_producer.py

CMD ["python", "producer.py"]
