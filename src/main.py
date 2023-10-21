from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import LongType, StructField, StructType, StringType, DoubleType, FloatType
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--lake_bucket", help="bucket where lake is located.")

args = parser.parse_args()
lake_bucket = args.lake_bucket

conf = (
    SparkConf()
    .setAppName('read_from_iceberg')
    .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .set('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')
    .set('spark.sql.catalog.spark_catalog.type', 'hive')
    .set(f'spark.sql.catalog.dev', 'org.apache.iceberg.spark.SparkCatalog')
    .set(f'spark.sql.catalog.dev.type', 'hive')
    .set(f'spark.sql.warehouse.dir', lake_bucket)
)
spark = SparkSession.builder.enableHiveSupport().config(conf=conf).getOrCreate()

df = spark.table("dev.lakehouse.trips").show()