
"""deprecated: HBase connector for storing and retrieving news articles."""
import logging
import happybase

logger = logging.getLogger(__name__)

class HBaseConnector:
    def __init__(self, host: str, port: int, table_name: str, column_family: str = 'cf'):
        self.connection = happybase.Connection(host=host, port=port)
        self.table_name = table_name
        self.column_family = column_family
        self._ensure_table_exists()
        logger.info("HBaseConnector initialized")
    
    def _ensure_table_exists(self):

        tables = self.connection.tables()

        if self.table_name not in tables:
            self.connection.create_table(
                self.table_name,
                {
                    self.column_family: dict(
                    max_versions=3,
                    block_cache_enabled=True,
                    bloom_filter_type='ROW'
                    )
                }
            )
            logger.info(f"Table '{self.table_name}' created with column family '{self.column_family}'")
        self.table = self.connection.table(self.table_name)
