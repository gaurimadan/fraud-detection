FROM apache/spark:3.5.5-python3

USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.10
RUN mkdir -p /home/spark/.ivy2/cache /tmp/spark-checkpoints \
    && chown -R 185:0 /home/spark/.ivy2 /tmp/spark-checkpoints
WORKDIR /app
COPY spark/streaming.py /app/streaming.py
USER 185
