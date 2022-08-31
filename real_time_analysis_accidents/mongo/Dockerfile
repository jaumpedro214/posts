FROM mongo:4.2.5
ENV MONGO_INITDB_ROOT_USERNAME=mongo
ENV MONGO_INITDB_ROOT_PASSWORD=mongo
ENV MONGO_REPLICA_SET_NAME=replica-set
CMD ["--replSet", "replica-set", "--bind_ip_all"]