FROM docker.io/bitnami/spark:3.3.1

COPY *.jar $SPARK_HOME/jars

COPY ./src/credentials/*.json $SPARK_HOME/secrets
ENV GOOGLE_APPLICATION_CREDENTIALS=$SPARK_HOME/secrets/gcp-credentials.json

RUN pip install delta-spark