mkdir -p ./data/
mkdir -p ./src/credentials
echo YOUR_BUCKET_NAME > ./src/credentials/bucket_name.txt

chmod -R 777 ./src
chmod -R 777 ./data

wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar