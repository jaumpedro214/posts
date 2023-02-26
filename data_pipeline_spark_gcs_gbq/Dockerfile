FROM docker.io/bitnami/spark:3.3.1

COPY *.jar $SPARK_HOME/jars

RUN mkdir -p $SPARK_HOME/secrets
COPY ./src/credentials/gcp-credentials.json $SPARK_HOME/secrets/gcp-credentials.json
ENV GOOGLE_APPLICATION_CREDENTIALS=$SPARK_HOME/secrets/gcp-credentials.json

RUN pip install delta-spark