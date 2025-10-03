from pyspark.sql import SparkSession
from pyspark.sql.functions import col, days,DataFrame
from pyspark.sql.types import StructType
from src.com.itquetzali.news.models.models import StorageMetrics
from datetime import datetime  
import logging

logger = logging.getLogger(__name__)

class IcebergConnector:
    def __init__(self, config: dict, schema:StructType, spark:SparkSession):
        self.config = config
        self.schema = schema
        self.spark = spark
        self.__verify_if_table_exists()

    def __verify_if_table_exists(self):
        logger.info("Checking if Iceberg table exists or needs to be created.")
        try:
            tables = self.spark.sql("SHOW TABLES in demo.news_catalog")
            table_exists = tables.filter(col("tableName") == self.config["iceberg"]["table_name"]).count() > 0
            if not table_exists:
                msg = f"Table {self.config["iceberg"]["table_name"]} does not exist"
                logger.error(msg)
                raise ValueError(msg)
            else:
                logger.info("Table exists")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise
    def store_dataframe(self, df:DataFrame):
        #metrics = StorageMetrics()
        start_date = datetime.now()

        try:
            df.writeTo(f"demo.news_catalog.{self.config['iceberg']['table_name']}").append()
            df.write \
                .format(self.config['iceberg']['format']) \
                .mode("overwrite") \
                .save(f"demo.news_catalog.{self.config['iceberg']['table_name']}")
            result = df.persist()
            result.show()
          #  metrics.iceberg_count = df.count()
            logger.info(f"Data appended to table {self.config['iceberg']['table_name']} successfully.")
        except Exception as e:
            logger.error(f"Error writing to table: {e}")
            
        
        #metrics.total_articles = metrics.iceberg_count
        #metrics.processing_time_ms = (datetime.now() - start_date).total_seconds() * 1000
        #metrics.last_processed = datetime.now()
        #return metrics
    
