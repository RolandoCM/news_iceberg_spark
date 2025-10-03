
"""deprecated: Cassandra connector for storing and retrieving news articles."""
import logging
from typing import List, Optional
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session
from cassandra.policies import RoundRobinPolicy
from src.com.itquetzali.news.models.models import NewsArticle
from datetime import datetime


logger = logging.getLogger(__name__)

class CassandraConnector:

    def __init__(self, hosts: List[str], port:int, keyspace:str):
        self.hosts = hosts
        self.port = port
        self.keyspace = keyspace
        self.session = Optional[Session] = None
        self.connect()
        self.setup_schema()

    def connect(self):
        """Establish connection to Cassandra cluster"""
        
        try:
            auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
            cluster = Cluster(
                self.hosts, 
                port=self.port, 
                auth_provider=auth_provider,
                load_balancing_policy=RoundRobinPolicy(),
                protocol_version=4
            )
            self.session = cluster.connect()
            logger.info("Connected to Cassandra cluster")
        except Exception as e:
            logger.error(f"Error setting up auth provider: {e}")
            raise
    
    def setup_schema(self):
        """Create keyspace and table if they do not exist"""
        try:
            self.session = self.cluster.connect()
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
                WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }}
            """)

            self.session.set_keyspace(self.keyspace)
            self.session.execute("""
                CREATE TABLE IF NOT EXISTS news_articles (
                    id UUID PRIMARY KEY,
                    title TEXT,
                    content TEXT,
                    summary TEXT,
                    source TEXT,
                    author TEXT,
                    url TEXT,
                    published_at TIMESTAMP,
                    category TEXT,
                    language TEXT,
                    tags LIST<TEXT>
                )
            """)
            self.session.execute("""CREATE INDEX IF NOT EXISTS ON news_articles (published_at)""")
            self.session.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS recent_articles AS SELECT * FROM news_articles
                                 WHERE published_at IS NOT NULL AND id IS NOT NULL
                                 PRIMARY KEY (published_at, id)
                                 WITH CLUSTERING ORDER BY (published_at DESC);""")
            logger.info("Cassandra keyspace and table are set up")
        except Exception as e:
            logger.error(f"Error setting up schema: {e}")
            raise
    
    def store_article(self, article: NewsArticle):
        """Insert a news article into the Cassandra table"""
        try:
            insert_query = """
                INSERT INTO news_articles (id, title, content, summary, source, author, url, published_at, category, language, tags)
                VALUES (uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.session.execute(insert_query, (article.title, article.content, article.summary, article.source,
                                                article.author, article.url, article.published_at, article.category,
                                                article.language, article.tags, datetime.now()))
            logger.info("Article stored in Cassandra")
        except Exception as e:
            logger.error(f"Error storing article: {e}")
            raise

    def store_articles_batch():
        pass   
    def get_article(self, article_id:str, category:Optional[str]=None) -> Optional[NewsArticle]:
        pass

    def get_recent_articles(self, limit:int=10) -> List[NewsArticle]:
        pass
    def cleanup_old_articles(self, days:int=30):
        pass
    def close(self):
        if self.session:
            self.session.shutdown()
            logger.info("Cassandra session closed")