from pyspark.sql import SparkSession
from error_logging.base_logger import logger
from pyspark.sql.utils import AnalysisException

sspark = SparkSession.builder \
    .appName("billboard_artists") \
    .config("spark.jars.packages", ",".join([
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "software.amazon.awssdk:bundle:2.20.18",
        "org.slf4j:slf4j-simple:1.7.36"
    ])) \
    .config("spark.sql.catalog.restcat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.restcat.catalog-impl", "org.apache.iceberg.rest.RESTCatalog") \
    .config("spark.sql.catalog.restcat.uri", "http://localhost:8181") \
    .config("spark.sql.catalog.restcat.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.restcat.warehouse", "s3://warehouse/") \
    .config("spark.sql.catalog.restcat.s3.endpoint", "http://localhost:9000") \
    .config("spark.sql.catalog.restcat.s3.path-style-access", "true") \
    .config("spark.sql.catalog.restcat.s3.access-key-id", "admin") \
    .config("spark.sql.catalog.restcat.s3.secret-access-key", "password") \
    .config("spark.sql.catalog.restcat.s3.region", "us-east-1") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
    .config("spark.executorEnv.AWS_REGION", "us-east-1") \
    .config("spark.sql.catalog.restcat.write.format.default", "parquet") \
    .getOrCreate()

class BillboardArist:
    _table_name = None
    _schema = 'billboard_db'

    @classmethod
    def _get_spark_session(cls):
        return SparkSession.getActiveSession()

    @classmethod
    def create_table(cls, table_name, data: list[dict]):
        cls._table_name = table_name

        df = cls._get_spark_session().createDataFrame(data)
        if df is None:
            logger.error("Spark session could not be created.")

        if cls._table_name is None or cls._schema is None:
            logger.error("Table name or schema is not defined.")

        spark = cls._get_spark_session()

        if check_table_exists(spark,"restcat", f"{cls._schema}", table_name):
            df_all_artists = spark.read.format("iceberg").load(f"restcat.{cls._schema}.{cls._table_name}")
            artists = df_all_artists.select("artist_name").distinct()
            df_artists = df.join(artists, on="artist_name", how="left_anti")
            if df_artists.count() > 0:
                df_artists.writeTo(f"restcat.{cls._schema}.{cls._table_name}").append()
            else:
                logger.info("No new artists to add.")
        else:
            logger.warning(f"Table {cls._table_name} does not exist. Creating a new table.")
            df.writeTo(f"restcat.{cls._schema}.{cls._table_name}").create()
            logger.info("new artists added.")
     
            

    @classmethod
    def read_table(cls, table_name):
        if cls._table_name is None or cls._schema is None:
            logger.error("Table name is not defined.")
            return None

        df = cls._get_spark_session().table(f"restcat.{cls._schema}.{cls._table_name}")
        if df is None:
            logger.error(f"Table {cls._table_name} could not be read.")

        return df

def check_table_exists(spark, catalog, schema, table) -> bool:
    try:
        result = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").filter(f"tableName = '{table}'")
        return result.count() > 0
    except Exception as e:
        logger.warning(f"Error checking table existence: {e}")
        return False

  
  

