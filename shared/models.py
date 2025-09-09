"""Shared data models for the Bluesky streaming pipeline.

This module contains the core data structures used throughout the pipeline
for representing posts, trends, and aggregated keyword counts.
"""

from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass, field


@dataclass
class PostData:
    """Represents a social media post from Bluesky firehose.
    
    This model captures the essential information from a Bluesky post
    that will be processed through the streaming pipeline.
    
    Attributes:
        uri: Unique identifier for the post in ATProto format
        author_did: Bluesky DID (Decentralized Identifier) of the author
        text: The actual content/text of the post
        created_at: Timestamp when the post was created
        language: Optional language code (ISO 639-1) detected from content
        reply_to: Optional URI of the post this is replying to
        mentions: List of DIDs mentioned in the post
        hashtags: List of hashtags extracted from the post text
    """
    
    uri: str
    author_did: str
    text: str
    created_at: datetime
    language: Optional[str] = None
    reply_to: Optional[str] = None
    mentions: List[str] = field(default_factory=list)
    hashtags: List[str] = field(default_factory=list)
    
    def __post_init__(self) -> None:
        """Ensure lists are properly initialized."""
        if self.mentions is None:
            self.mentions = []
        if self.hashtags is None:
            self.hashtags = []


@dataclass
class TrendAlert:
    """Represents a detected trending keyword or topic.
    
    This model contains the statistical information about a trending
    keyword detected by the stream processing engine.
    
    Attributes:
        keyword: The trending keyword or phrase
        frequency: Total count of occurrences in the time window
        growth_rate: Rate of growth compared to previous window (percentage)
        confidence_score: Statistical confidence score (0.0 to 1.0)
        window_start: Start timestamp of the analysis window
        window_end: End timestamp of the analysis window
        sample_posts: List of sample post URIs containing this keyword
        unique_authors: Number of unique authors who used this keyword
        rank: Ranking position among current trends (1 = most trending)
    """
    
    keyword: str
    frequency: int
    growth_rate: float
    confidence_score: float
    window_start: datetime
    window_end: datetime
    sample_posts: List[str]
    unique_authors: int
    rank: int
    
    def __post_init__(self) -> None:
        """Validate trend alert data."""
        if self.frequency < 0:
            raise ValueError("Frequency must be non-negative")
        if not 0.0 <= self.confidence_score <= 1.0:
            raise ValueError("Confidence score must be between 0.0 and 1.0")
        if self.unique_authors < 0:
            raise ValueError("Unique authors count must be non-negative")
        if self.rank < 1:
            raise ValueError("Rank must be positive")
        if self.window_start >= self.window_end:
            raise ValueError("Window start must be before window end")


@dataclass
class WindowedKeywordCount:
    """Represents keyword frequency data within a specific time window.
    
    This model is used for intermediate processing in the stream processing
    engine to track keyword counts across sliding time windows.
    
    Attributes:
        keyword: The original keyword as extracted from posts
        count: Number of occurrences in this time window
        window_start: Start timestamp of the time window
        window_end: End timestamp of the time window
        unique_authors: Number of unique authors who used this keyword
        normalized_keyword: Processed/normalized version for matching
    """
    
    keyword: str
    count: int
    window_start: datetime
    window_end: datetime
    unique_authors: int
    normalized_keyword: str
    
    def __post_init__(self) -> None:
        """Validate windowed keyword count data."""
        if self.count < 0:
            raise ValueError("Count must be non-negative")
        if self.unique_authors < 0:
            raise ValueError("Unique authors count must be non-negative")
        if self.window_start >= self.window_end:
            raise ValueError("Window start must be before window end")
        if not self.normalized_keyword:
            raise ValueError("Normalized keyword cannot be empty")