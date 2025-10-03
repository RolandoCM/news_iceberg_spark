
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
from datetime import datetime, timedelta
from src.com.itquetzali.news.connector.iceberg_connector import IcebergConnector

logger = logging.getLogger(__name__)
class SparkNewsProcessor:
    def __init__(self, config:dict):
        self.config = config
        self.spark = self._create_spark_session()
        self.spark.sparkContext.setLogLevel("WARN")
        
        ## create enricher

        self.schema = StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("author", StringType(), True),
            StructField("url", StringType(), True),
            StructField("published", TimestampType(), True),
            StructField("category", StringType(), True),
            StructField("language", StringType(), True),
            StructField("tags", StringType(), True),
            StructField("processing_timestamp", TimestampType(), True)
        ])
        self.iceberg = IcebergConnector(self.config, self.schema, self.spark)
    def _create_spark_session(self) -> SparkSession:
        """Create and return a Spark session."""
        return SparkSession.builder \
            .appName(self.config["spark"]["app_name"]) \
            .master(self.config["spark"]["master"]) \
            .config("spark.executor.memory", self.config["spark"]["executor_memory"]) \
            .config("spark.driver.memory", self.config["spark"]["driver_memory"]) \
            .config("spark.sql.shuffle.partitions", self.config["spark"]["shuffle_partitions"]) \
            .config("spark.cassandra.connection.host", self.config["cassandra"]["host"]) \
            .config("spark.cassandra.connection.port", self.config["cassandra"]["port"]) \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0," \
            "org.apache.iceberg:iceberg-spark-extensions-3.5_2.12:1.9.0," \
            "org.apache.iceberg:iceberg-aws-bundle:1.9.0," \
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1," \
            "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1," \
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.10.0") \
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.demo.type", "rest") \
            .config("spark.sql.catalog.demo.uri", "http://localhost:8181") \
            .config("spark.sql.demo.s3.endpoint", "http://localhost:9000") \
            .config("spark.sql.catalog.demo.warehouse","s3a://warehouse/wh/")\
            .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.defaultCatalog","demo") \
            .config("spark.eventLog.enabled", "true")\
            .config("spark.sql.adapive.enabled", "true") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
            .config("spark.eventLog.dir", "/home/rolando/devs/news_analitics/news-store-pipeline/iceberg/spark-events") \
            .config("spark.history.fs.logDirectory","/home/rolando/devs/news_analitics/news-store-pipeline/iceberg/spark-events")\
            .getOrCreate()
    def process_kafka_stream(self):
        """Proess data from Kafka stream."""

        try:
            rdd = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", self.config["kafka"]["topics"]) \
                .load()
            
            logger.info(f"Kafka stream created successfully.")
            # Cast 'key' and 'value' columns to STRING
            df_s = rdd.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                .withColumn("data", from_json(col("value"), self.schema)) \
                .select("key","data.*")
            # Now, df_string has 'key' and 'value' as StringType columns
            df = df_s \
                .writeStream \
                .foreachBatch(self.__process_rdd) \
                .queryName("news_articles") \
                .option("truncate", "false") \
                .start()
            df.awaitTermination()
        except Exception as e:
            logger.error(f"Error creating Kafka stream: {e}")
            raise
        finally:
          df.stop()
            #scc.stop(stopSparkContext=True, stopGraceFully=True)
    def __process_rdd(self, df, batch_id):
        """Process each RDD from the Kafka stream."""
        logger.info(f"Processing new RDD with batch ID: {batch_id}")
        try:

            if df.isEmpty():
                logger.info("Received empty RDD, skipping processing.")
                return
            df.printSchema()
            df.withColumn("processing_timestamp", current_timestamp())

            if self.config["enrichment"]["enabled"]:
                # Enrich data
                pass
            
            # Determine storage based on time
            cutoff_time = datetime.now() - timedelta(days=self.config["retention"]["hot_data_days"])
            cutoff_timestamp = lit(cutoff_time.isoformat())
            logger.info(f"Cutoff timestamp for hot data: {cutoff_time.isoformat()}, hot data days: {self.config['retention']['hot_data_days']}, current time: {datetime.now().isoformat()} ")
            hot_data = df.filter(col("published") >= cutoff_timestamp)
            analythics_data = df
            ## call storage methods 
            #self.__write_to_cassandra(hot_data)
            self.iceberg.store_dataframe(analythics_data)
            logger.info(f"processing batch with {analythics_data.count()} total records and {hot_data.count()} hot records")
            
            #iceberg.store_dataframe(analythics_data)
            logger.info(f"Batch ID {batch_id} processed successfully.") 
            
        except Exception as e:
            logger.error(f"Error processing RDD: {e}")
            raise
            
    def __write_to_cassandra(self, df):
        """Store data in cassandra."""
        try:
            if df.isEmpty():
                logger.info("No hot data to write to Cassandra.")
                return

            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=self.config["cassandra"]["table"], keyspace=self.config["cassandra"]["keyspace"]) \
                .mode("append") \
                .save()
            logger.info("Data written to Cassandra successfully.")
        except Exception as e:
            logger.error(f"Error writing to Cassandra: {e}")
            raise

    def __store_in_iceberg(self, df):
        """Store data in iceberg."""
        pass
    def close(self):
        pass

    def compat_tables(self):
        pass