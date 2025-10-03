from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


"""class ArticleCategory(BaseModel):
    POLITICS = "politics"
    SPORTS = "sports"
    TECHNOLOGY = "technology"
    BUSINESS = "business"
    ENTERTAINMENT = "entertainment"
    HEALTH = "health"
    SCIENCE = "science"
    GENERAL = "general"
    OTHER = "other"""
class NewsArticle(BaseModel):
    id: str
    title:str
    description:str
    author: Optional[str] = None
    url: str
    published: datetime
    category: Optional[str] = None
    language: str
    tags:List = Field(default_factory=list)
    processing_timestamp: datetime = Field(default_factory=datetime.now)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()   # Custom serialization for datetime
        }

class EnrichedNewsArticle(NewsArticle):
    entities: Optional[List[str]] = Field(default_factory=list)
    sentiment: Optional[str] = None
    topics: Optional[List[str]] = Field(default_factory=list)
    keywords: Optional[List[str]] = Field(default_factory=list)
    readability_score: Optional[float] = None
    processing_timestamp: datetime = Field(default_factory=datetime.now)

class StorageMetrics(BaseModel):
    total_articles: int
    cassandra_count: int
    iceberg_count: int
    last_processed: datetime = Field(default_factory=datetime.now)
    processing_time_ms: float