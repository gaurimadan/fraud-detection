FROM apache/spark:3.5.5-python3

USER root
COPY model/requirements.txt /tmp/model-requirements.txt
RUN pip install --no-cache-dir -r /tmp/model-requirements.txt
WORKDIR /app
COPY model/train_model.py /app/train_model.py
COPY tests/test_model_training.py /app/tests/test_model_training.py
