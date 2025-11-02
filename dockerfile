FROM python:3.11-slim

WORKDIR /app

# Install Java 21 (supported by PySpark 3.5+)
RUN apt-get update && apt-get install -y openjdk-21-jre procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY . /app

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV KAGGLE_CONFIG_DIR=/root/.kaggle




CMD ["python", "src/run_pipeline.py"]
