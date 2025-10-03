create keyspace if not exists news_keyspace
with replication  = {
	'class': 'SimpleStrategy',
	'replication_factor': 1
	};

use news_keyspace;

CREATE TABLE IF NOT EXISTS news_articles (
                    id UUID PRIMARY KEY,
                    key TEXT,
                    title TEXT,
                    description TEXT,
                    author TEXT,
                    url TEXT,
                    published TIMESTAMP,
                    category TEXT,
                    processing_timestamp TIMESTAMP,
                    language TEXT,
                    tags LIST<TEXT>);

select * from news_articles;
