"""
Bluesky Firehose Client

This module implements the BlueskyFirehoseClient class that connects to the
Bluesky ATProto firehose and extracts post data for processing.
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, AsyncGenerator
import re

from atproto import (
    AsyncFirehoseSubscribeReposClient, 
    parse_subscribe_repos_message,
    models,
    firehose_models,
    CAR,
    AtUri
)
from atproto.exceptions import AtProtocolError
import structlog

from shared.models import PostData


class BlueskyFirehoseClient:
    """Client for connecting to Bluesky firehose and extracting post data.
    
    This client handles connection management, data extraction, and resilience
    features like cursor-based resumption and exponential backoff.
    """
    
    def __init__(
        self,
        max_retries: int = 10,
        initial_backoff: float = 1.0,
        max_backoff: float = 300.0,
        backoff_multiplier: float = 2.0
    ) -> None:
        """Initialize the Bluesky firehose client.
        
        Args:
            max_retries: Maximum number of connection retry attempts
            initial_backoff: Initial backoff delay in seconds
            max_backoff: Maximum backoff delay in seconds
            backoff_multiplier: Multiplier for exponential backoff
        """
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_multiplier = backoff_multiplier
        
        # Connection state
        self._client: Optional[AsyncFirehoseSubscribeReposClient] = None
        self._cursor: Optional[str] = None
        self._is_connected = False
        self._retry_count = 0
        
        # Logging with correlation IDs
        self.logger = structlog.get_logger(__name__)
        self._correlation_id = str(uuid.uuid4())
        
        # Statistics
        self._posts_processed = 0
        self._connection_attempts = 0
        self._last_message_time: Optional[datetime] = None
        
        # Regex patterns for text processing
        self._hashtag_pattern = re.compile(r'#\w+')
        self._mention_pattern = re.compile(r'@[\w.-]+')
    
    async def connect_to_firehose(self, cursor: Optional[str] = None) -> None:
        """Connect to the Bluesky firehose with optional cursor resumption.
        
        Args:
            cursor: Optional cursor position to resume from
            
        Raises:
            AtProtocolError: If connection fails after all retries
        """
        self._correlation_id = str(uuid.uuid4())
        self._cursor = cursor
        self._connection_attempts += 1
        
        log = self.logger.bind(
            correlation_id=self._correlation_id,
            cursor=cursor,
            attempt=self._connection_attempts
        )
        
        log.info("Attempting to connect to Bluesky firehose")
        
        try:
            self._client = AsyncFirehoseSubscribeReposClient()
            self._is_connected = True
            self._retry_count = 0
            
            log.info("Successfully connected to Bluesky firehose")
            
        except Exception as e:
            self._is_connected = False
            log.error("Failed to connect to firehose", error=str(e))
            raise AtProtocolError(f"Connection failed: {e}")
    
    async def process_firehose_stream(self) -> AsyncGenerator[PostData, None]:
        """Process the firehose stream and yield PostData objects.
        
        Yields:
            PostData: Extracted post data from firehose messages
            
        Raises:
            AtProtocolError: If stream processing fails
        """
        if not self._client or not self._is_connected:
            raise AtProtocolError("Client not connected. Call connect_to_firehose() first.")
        
        log = self.logger.bind(correlation_id=self._correlation_id)
        log.info("Starting firehose stream processing")
        
        # Create a queue to collect messages from the callback
        message_queue = asyncio.Queue()
        
        def on_message_callback(message):
            """Callback to handle incoming messages"""
            try:
                asyncio.create_task(message_queue.put(message))
            except Exception as e:
                log.warning("Error queuing message", error=str(e))
        
        try:
            # Start the firehose client with callback
            await self._client.start(on_message_callback)
            
            # Process messages from the queue
            while self._is_connected:
                try:
                    # Wait for message with timeout
                    message_bytes = await asyncio.wait_for(message_queue.get(), timeout=30.0)
                    
                    # Parse the message
                    parsed_message = parse_subscribe_repos_message(message_bytes)
                    
                    # Update cursor for resumption
                    if hasattr(parsed_message, 'seq'):
                        self._cursor = str(parsed_message.seq)
                    
                    # Process commits that contain posts
                    if hasattr(parsed_message, 'commit') and parsed_message.commit:
                        async for post_data in self._process_commit(parsed_message.commit):
                            self._posts_processed += 1
                            self._last_message_time = datetime.now(timezone.utc)
                            yield post_data
                            
                except asyncio.TimeoutError:
                    log.debug("No messages received in 30 seconds, continuing...")
                    continue
                except Exception as e:
                    log.warning("Error processing firehose message", error=str(e))
                    continue
                    
        except Exception as e:
            self._is_connected = False
            log.error("Firehose stream processing failed", error=str(e))
            raise AtProtocolError(f"Stream processing failed: {e}")
    
    async def _process_commit(self, commit: Any) -> AsyncGenerator[PostData, None]:
        """Process a commit message and extract post data.
        
        Args:
            commit: Commit message from firehose
            
        Yields:
            PostData: Extracted post data
        """
        if not hasattr(commit, 'ops') or not commit.ops:
            return
        
        log = self.logger.bind(correlation_id=self._correlation_id)
        
        for op in commit.ops:
            try:
                # Only process create operations for posts
                if (hasattr(op, 'action') and op.action == 'create' and
                    hasattr(op, 'path') and op.path and 
                    op.path.startswith('app.bsky.feed.post/')):
                    
                    if hasattr(op, 'record') and op.record:
                        post_data = await self._extract_post_data(op.record, commit)
                        if post_data:
                            yield post_data
                            
            except Exception as e:
                log.warning("Error processing commit operation", error=str(e))
                continue
    
    async def _extract_post_data(self, record: Any, commit: Any) -> Optional[PostData]:
        """Extract PostData from a firehose record.
        
        Args:
            record: Post record from firehose
            commit: Commit containing the record
            
        Returns:
            PostData object or None if extraction fails
        """
        try:
            # Extract basic post information
            text = getattr(record, 'text', '')
            if not text or not isinstance(text, str):
                return None
            
            # Extract timestamp
            created_at_str = getattr(record, 'createdAt', '')
            if created_at_str:
                try:
                    created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                except (ValueError, AttributeError):
                    created_at = datetime.now(timezone.utc)
            else:
                created_at = datetime.now(timezone.utc)
            
            # Extract author DID from commit
            author_did = getattr(commit, 'repo', '')
            if not author_did:
                return None
            
            # Generate URI (simplified version)
            uri = f"at://{author_did}/app.bsky.feed.post/{uuid.uuid4()}"
            
            # Extract language if available
            language = None
            if hasattr(record, 'langs') and record.langs:
                language = record.langs[0] if isinstance(record.langs, list) else str(record.langs)
            
            # Extract reply information
            reply_to = None
            if hasattr(record, 'reply') and record.reply:
                if hasattr(record.reply, 'parent') and hasattr(record.reply.parent, 'uri'):
                    reply_to = record.reply.parent.uri
            
            # Extract mentions and hashtags from text
            mentions = self._extract_mentions(text)
            hashtags = self._extract_hashtags(text)
            
            return PostData(
                uri=uri,
                author_did=author_did,
                text=text,
                created_at=created_at,
                language=language,
                reply_to=reply_to,
                mentions=mentions,
                hashtags=hashtags
            )
            
        except Exception as e:
            self.logger.warning(
                "Failed to extract post data",
                correlation_id=self._correlation_id,
                error=str(e)
            )
            return None
    
    def _extract_mentions(self, text: str) -> List[str]:
        """Extract mentions from post text.
        
        Args:
            text: Post text content
            
        Returns:
            List of mentioned usernames (without @ symbol)
        """
        mentions = self._mention_pattern.findall(text)
        return [mention[1:] for mention in mentions]  # Remove @ symbol
    
    def _extract_hashtags(self, text: str) -> List[str]:
        """Extract hashtags from post text.
        
        Args:
            text: Post text content
            
        Returns:
            List of hashtags (without # symbol)
        """
        hashtags = self._hashtag_pattern.findall(text)
        return [hashtag[1:] for hashtag in hashtags]  # Remove # symbol
    
    async def handle_reconnection(self, cursor: Optional[str] = None) -> None:
        """Handle reconnection with exponential backoff.
        
        Args:
            cursor: Optional cursor to resume from
            
        Raises:
            AtProtocolError: If all reconnection attempts fail
        """
        if cursor:
            self._cursor = cursor
        
        # Reset retry count for new reconnection attempt
        self._retry_count = 0
        
        log = self.logger.bind(
            correlation_id=self._correlation_id,
            cursor=self._cursor
        )
        
        while self._retry_count < self.max_retries:
            try:
                # Calculate backoff delay
                backoff_delay = min(
                    self.initial_backoff * (self.backoff_multiplier ** self._retry_count),
                    self.max_backoff
                )
                
                log.info("Attempting reconnection", 
                        backoff_delay=backoff_delay,
                        retry_count=self._retry_count)
                
                if self._retry_count > 0:
                    await asyncio.sleep(backoff_delay)
                
                await self.connect_to_firehose(self._cursor)
                log.info("Reconnection successful")
                return
                
            except Exception as e:
                self._retry_count += 1
                log.warning(
                    "Reconnection attempt failed",
                    error=str(e),
                    retry_count=self._retry_count
                )
                
                if self._retry_count >= self.max_retries:
                    log.error("All reconnection attempts exhausted")
                    raise AtProtocolError(f"Reconnection failed after {self.max_retries} attempts")
    
    async def disconnect(self) -> None:
        """Disconnect from the firehose."""
        if self._client:
            try:
                # ATProto client doesn't have explicit disconnect, just clean up
                self._client = None
                self._is_connected = False
                
                self.logger.info(
                    "Disconnected from firehose",
                    correlation_id=self._correlation_id,
                    posts_processed=self._posts_processed
                )
            except Exception as e:
                self.logger.warning("Error during disconnect", error=str(e))
    
    @property
    def is_connected(self) -> bool:
        """Check if client is connected to firehose."""
        return self._is_connected
    
    @property
    def current_cursor(self) -> Optional[str]:
        """Get current cursor position."""
        return self._cursor
    
    @property
    def posts_processed(self) -> int:
        """Get number of posts processed."""
        return self._posts_processed
    
    @property
    def last_message_time(self) -> Optional[datetime]:
        """Get timestamp of last processed message."""
        return self._last_message_time
    
    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics.
        
        Returns:
            Dictionary containing client statistics
        """
        return {
            'is_connected': self._is_connected,
            'posts_processed': self._posts_processed,
            'connection_attempts': self._connection_attempts,
            'retry_count': self._retry_count,
            'current_cursor': self._cursor,
            'last_message_time': self._last_message_time.isoformat() if self._last_message_time else None,
            'correlation_id': self._correlation_id
        }