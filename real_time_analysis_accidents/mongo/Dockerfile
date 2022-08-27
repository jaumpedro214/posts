FROM mongo:4.2.5
ENV MONGO_INITDB_ROOT_USERNAME=mongo
ENV MONGO_INITDB_ROOT_PASSWORD=mongo
ENV MONGO_REPLICA_SET_NAME=replica-set
#RUN ["mongo", "-u", "mongo", "-p", "mongo", "--eval", "rs.initiate()"]
CMD ["--replSet", "replica-set", "--bind_ip_all"]