#!/bin/bash

export APP_NAME=iceberg-dataproc-batch

export VERSION=0.0.1

export PROJECT_ID= # add your project ID

export REGION=us-central1

export SRC_WITH_DEPS=src_with_deps

mkdir -p ./dist
poetry update
poetry export -f requirements.txt --without-hashes -o requirements.txt
poetry run pip install . -r requirements.txt -t ${SRC_WITH_DEPS}
cd ${SRC_WITH_DEPS} || exit
find . -name "*.pyc" -delete
zip -x "*.git*" -x "*.DS_Store" -x "*.pyc" -x "*/*__pycache__*/" -x ".idea*" -r ../dist/${SRC_WITH_DEPS}.zip .
rm -rf ../${SRC_WITH_DEPS}
rm -f ../requirements.txt
cp ../src/main.py ../dist
mv ../dist/${SRC_WITH_DEPS}.zip ../dist/${APP_NAME}_${VERSION}.zip

export JOB_BUCKET= # the name for the job bucket.

cd ..
gsutil cp -r dist/main.py gs://${JOB_BUCKET}/${APP_NAME}/${VERSION}/main.py
gsutil cp -r dist/${APP_NAME}_${VERSION}.zip gs://${JOB_BUCKET}/${APP_NAME}/${VERSION}/${APP_NAME}_${VERSION}.zip

export METASTORE_NAME=iceberg-metastore

# shellcheck disable=SC2155
export METASTORE_ENDPOINT=$(gcloud metastore services describe ${METASTORE_NAME} --project ${PROJECT_ID} --location=us-central1 --format="value(endpointUri)")

# shellcheck disable=SC2155
export METASTORE_WAREHOUSE=$(gcloud metastore services describe ${METASTORE_NAME} --project ${PROJECT_ID} --location=us-central1 --format="value(hiveMetastoreConfig.configOverrides[hive.metastore.warehouse.dir])")

export DATAPROC_SA_NAME=dataproc-worker-sa

gcloud iam service-accounts create ${DATAPROC_SA_NAME} --description="Service account for a dataproc worker" --display-name=${DATAPROC_SA_NAME} --project ${PROJECT_ID}

gcloud projects add-iam-policy-binding ${PROJECT_ID} --member=serviceAccount:${DATAPROC_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com --role=roles/dataproc.worker

#Coping required jar dependancies to Job bucket.
wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.1/postgresql-42.5.1.jar
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.13/1.4.0/iceberg-spark-runtime-3.3_2.13-1.4.0.jar
gsutil cp postgresql-42.5.1.jar gs://${JOB_BUCKET}/dependencies/
gsutil cp iceberg-spark-runtime-3.3_2.13-1.4.0.jar gs://${JOB_BUCKET}/dependencies/
rm iceberg-spark-runtime-3.3_2.13-1.4.0.jar
rm postgresql-42.5.1.jar

export LAKE_BUCKET="gs://iceberg-lakehouse-370f19c2/datalake/"

gcloud beta dataproc batches submit --project ${PROJECT_ID} --region ${REGION} \
   --service-account=$SERVICE_ACCOUNT_EMAIL pyspark \
   gs://${JOB_BUCKET}/${APP_NAME}/${VERSION}/main.py \
   --py-files=gs://${JOB_BUCKET}/${APP_NAME}/${VERSION}/${APP_NAME}_${VERSION}.zip \
   --network default \
   --version=2.0 \
   --properties spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4,spark.app.name=batch_iceberg,spark.hive.metastore.uris=$METASTORE_ENDPOINT,spark.hive.metastore.warehouse.dir=$METASTORE_WAREHOUSE \
   --jars gs://$JOB_BUCKET/dependencies/postgresql-42.5.1.jar,gs://$JOB_BUCKET/dependencies/iceberg-spark-runtime-3.3_2.13-1.4.0.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
   --  --lake_bucket=${LAKE_BUCKET}

