
import logging
import yaml

from connector.spark_processor import SparkNewsProcessor
from threading import Thread
from time import sleep
from datetime import datetime, timedelta

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers = [
            logging.FileHandler('news_store_pipeline.log'),
            logging.StreamHandler()
        ]
    )

def load_config():
    """Load configuration from a YAML file."""
    with open('config/config.yml', 'r') as file:
        return yaml.safe_load(file)

def mantenance_worker(processor: SparkNewsProcessor, interval_hours: int = 24):
    """Perform periodic maintenance tasks."""
    while True:
        try:
            # Compact tables
            processor.compact_tables()
            
            # Run batch processing for yesterday's data
            yesterday = datetime.now() - timedelta(days=1)
            processor.run_batch_processing(
                yesterday.replace(hour=0, minute=0, second=0, microsecond=0),
                yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
            )
            
            sleep(interval_hours * 3600)
        except Exception as e:
            logging.error(f"Maintenance worker failed: {e}")
            sleep(3600)  # Retry after 1 hour
def main():
    """Backgound service to consume news articles from Kafka and store them in Iceberg."""
    setup_logging()

    logger = logging.getLogger(__name__)

    try:
        config = load_config()
        logger.info(f"Configuration loaded successfully: {config}")
        #processorNews = NewsProcessor(config)
        logger.info("NewsProcessor initialized successfully")   
        processor = SparkNewsProcessor(config)
        logger.info("SparkNewsProcessor initialized successfully")

        """mantenance_thread = Thread( 
            target=mantenance_worker, 
            args=(processor, config['maintenance']['interval_hours']), 
            daemon=True 
        )
        mantenance_thread.start()
        logger.info("Mantenance worker thread started")"""

        logger.info("starting kafka message processing")
        processor.process_kafka_stream()
    
        #processor.consume()
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Application failed: {e}")
    finally:
        if 'processor' in locals():
            processor.close()
        logger.info("Application stopped")

if __name__ == "__main__" and __package__ is None:
    __package__ = "com.itquetzali.news"
    main()