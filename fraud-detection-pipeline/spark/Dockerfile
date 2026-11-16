FROM apache/spark:3.5.5-python3

USER root
COPY model/requirements.txt /tmp/model-requirements.txt
RUN pip install --no-cache-dir psycopg2-binary==2.9.10 \
    && pip install --no-cache-dir -r /tmp/model-requirements.txt \
    && mkdir -p /home/spark/.ivy2/cache /tmp/spark-checkpoints \
    && chown -R 185:0 /home/spark/.ivy2 /tmp/spark-checkpoints
WORKDIR /app
COPY spark/streaming.py /app/streaming.py
COPY postgres/init.sql /app/init.sql
COPY render-model/ /models/
USER 185

CMD ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "1g", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.postgresql:postgresql:42.7.5", "/app/streaming.py"]
