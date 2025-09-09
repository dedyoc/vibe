"""
Unit tests for BlueskyFirehoseClient

Tests the firehose connection, data extraction, resilience features,
and error handling using mock ATProto client.
"""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from typing import List, Any

from atproto.exceptions import AtProtocolError

from bluesky_client import BlueskyFirehoseClient
from shared.models import PostData


class TestBlueskyFirehoseClient:
    """Test suite for BlueskyFirehoseClient."""
    
    @pytest.fixture
    def client(self) -> BlueskyFirehoseClient:
        """Create a BlueskyFirehoseClient instance for testing."""
        return BlueskyFirehoseClient(
            max_retries=3,
            initial_backoff=0.1,
            max_backoff=1.0,
            backoff_multiplier=2.0
        )
    
    @pytest.fixture
    def mock_firehose_client(self) -> AsyncMock:
        """Create a mock ATProto firehose client."""
        mock_client = AsyncMock()
        mock_client.get_messages = AsyncMock()
        return mock_client
    
    @pytest.fixture
    def sample_post_record(self) -> MagicMock:
        """Create a sample post record for testing."""
        record = MagicMock()
        record.text = "Hello world! This is a test post #testing @user123"
        record.createdAt = "2024-01-15T10:30:00.000Z"
        record.langs = ["en"]
        record.reply = None
        return record
    
    @pytest.fixture
    def sample_commit(self) -> MagicMock:
        """Create a sample commit for testing."""
        commit = MagicMock()
        commit.repo = "did:plc:test123456789"
        commit.seq = 12345
        
        # Create operation
        op = MagicMock()
        op.action = "create"
        op.path = "app.bsky.feed.post/test123"
        op.record = None  # Will be set in individual tests
        
        commit.ops = [op]
        return commit
    
    @pytest.fixture
    def sample_message(self, sample_commit: MagicMock) -> MagicMock:
        """Create a sample firehose message."""
        message = MagicMock()
        message.seq = 12345
        return message
    
    @pytest.mark.asyncio
    async def test_connect_to_firehose_success(self, client: BlueskyFirehoseClient) -> None:
        """Test successful connection to firehose."""
        with patch('bluesky_client.AsyncFirehoseSubscribeReposClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value = mock_client
            
            await client.connect_to_firehose()
            
            assert client.is_connected
            assert client._client is not None
            assert client._retry_count == 0
            mock_client_class.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_connect_to_firehose_with_cursor(self, client: BlueskyFirehoseClient) -> None:
        """Test connection with cursor resumption."""
        cursor = "test_cursor_123"
        
        with patch('bluesky_client.AsyncFirehoseSubscribeReposClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value = mock_client
            
            await client.connect_to_firehose(cursor=cursor)
            
            assert client.current_cursor == cursor
            assert client.is_connected
    
    @pytest.mark.asyncio
    async def test_connect_to_firehose_failure(self, client: BlueskyFirehoseClient) -> None:
        """Test connection failure handling."""
        with patch('bluesky_client.AsyncFirehoseSubscribeReposClient') as mock_client_class:
            mock_client_class.side_effect = Exception("Connection failed")
            
            with pytest.raises(AtProtocolError, match="Connection failed"):
                await client.connect_to_firehose()
            
            assert not client.is_connected
            assert client._client is None
    
    @pytest.mark.asyncio
    async def test_extract_post_data_success(
        self, 
        client: BlueskyFirehoseClient,
        sample_post_record: MagicMock,
        sample_commit: MagicMock
    ) -> None:
        """Test successful post data extraction."""
        post_data = await client._extract_post_data(sample_post_record, sample_commit)
        
        assert post_data is not None
        assert isinstance(post_data, PostData)
        assert post_data.text == "Hello world! This is a test post #testing @user123"
        assert post_data.author_did == "did:plc:test123456789"
        assert post_data.language == "en"
        assert "testing" in post_data.hashtags
        assert "user123" in post_data.mentions
        assert post_data.reply_to is None
    
    @pytest.mark.asyncio
    async def test_extract_post_data_with_reply(
        self,
        client: BlueskyFirehoseClient,
        sample_post_record: MagicMock,
        sample_commit: MagicMock
    ) -> None:
        """Test post data extraction with reply information."""
        # Add reply information
        reply_mock = MagicMock()
        parent_mock = MagicMock()
        parent_mock.uri = "at://did:plc:parent123/app.bsky.feed.post/parent456"
        reply_mock.parent = parent_mock
        sample_post_record.reply = reply_mock
        
        post_data = await client._extract_post_data(sample_post_record, sample_commit)
        
        assert post_data is not None
        assert post_data.reply_to == "at://did:plc:parent123/app.bsky.feed.post/parent456"
    
    @pytest.mark.asyncio
    async def test_extract_post_data_empty_text(
        self,
        client: BlueskyFirehoseClient,
        sample_post_record: MagicMock,
        sample_commit: MagicMock
    ) -> None:
        """Test post data extraction with empty text."""
        sample_post_record.text = ""
        
        post_data = await client._extract_post_data(sample_post_record, sample_commit)
        
        assert post_data is None
    
    @pytest.mark.asyncio
    async def test_extract_post_data_missing_author(
        self,
        client: BlueskyFirehoseClient,
        sample_post_record: MagicMock,
        sample_commit: MagicMock
    ) -> None:
        """Test post data extraction with missing author."""
        sample_commit.repo = ""
        
        post_data = await client._extract_post_data(sample_post_record, sample_commit)
        
        assert post_data is None
    
    @pytest.mark.asyncio
    async def test_extract_post_data_invalid_timestamp(
        self,
        client: BlueskyFirehoseClient,
        sample_post_record: MagicMock,
        sample_commit: MagicMock
    ) -> None:
        """Test post data extraction with invalid timestamp."""
        sample_post_record.createdAt = "invalid_timestamp"
        
        post_data = await client._extract_post_data(sample_post_record, sample_commit)
        
        assert post_data is not None
        # Should use current time as fallback
        assert isinstance(post_data.created_at, datetime)
    
    def test_extract_mentions(self, client: BlueskyFirehoseClient) -> None:
        """Test mention extraction from text."""
        text = "Hello @user1 and @user2.bsky.social! How are you @user3?"
        mentions = client._extract_mentions(text)
        
        expected_mentions = ["user1", "user2.bsky.social", "user3"]
        assert mentions == expected_mentions
    
    def test_extract_hashtags(self, client: BlueskyFirehoseClient) -> None:
        """Test hashtag extraction from text."""
        text = "This is a #test post with #multiple #hashtags!"
        hashtags = client._extract_hashtags(text)
        
        expected_hashtags = ["test", "multiple", "hashtags"]
        assert hashtags == expected_hashtags
    
    def test_extract_mentions_empty_text(self, client: BlueskyFirehoseClient) -> None:
        """Test mention extraction from empty text."""
        mentions = client._extract_mentions("")
        assert mentions == []
    
    def test_extract_hashtags_empty_text(self, client: BlueskyFirehoseClient) -> None:
        """Test hashtag extraction from empty text."""
        hashtags = client._extract_hashtags("")
        assert hashtags == []
    
    @pytest.mark.asyncio
    async def test_process_commit_success(
        self,
        client: BlueskyFirehoseClient,
        sample_commit: MagicMock,
        sample_post_record: MagicMock
    ) -> None:
        """Test successful commit processing."""
        sample_commit.ops[0].record = sample_post_record
        
        posts = []
        async for post in client._process_commit(sample_commit):
            posts.append(post)
        
        assert len(posts) == 1
        assert isinstance(posts[0], PostData)
        assert posts[0].text == sample_post_record.text
    
    @pytest.mark.asyncio
    async def test_process_commit_no_ops(self, client: BlueskyFirehoseClient) -> None:
        """Test commit processing with no operations."""
        commit = MagicMock()
        commit.ops = []
        
        posts = []
        async for post in client._process_commit(commit):
            posts.append(post)
        
        assert len(posts) == 0
    
    @pytest.mark.asyncio
    async def test_process_commit_non_post_operation(
        self,
        client: BlueskyFirehoseClient,
        sample_commit: MagicMock
    ) -> None:
        """Test commit processing with non-post operation."""
        sample_commit.ops[0].path = "app.bsky.actor.profile/test"
        
        posts = []
        async for post in client._process_commit(sample_commit):
            posts.append(post)
        
        assert len(posts) == 0
    
    @pytest.mark.asyncio
    async def test_process_commit_delete_operation(
        self,
        client: BlueskyFirehoseClient,
        sample_commit: MagicMock
    ) -> None:
        """Test commit processing with delete operation."""
        sample_commit.ops[0].action = "delete"
        
        posts = []
        async for post in client._process_commit(sample_commit):
            posts.append(post)
        
        assert len(posts) == 0
    
    @pytest.mark.asyncio
    async def test_handle_reconnection_success(self, client: BlueskyFirehoseClient) -> None:
        """Test successful reconnection."""
        with patch.object(client, 'connect_to_firehose') as mock_connect:
            mock_connect.return_value = None
            
            await client.handle_reconnection()
            
            mock_connect.assert_called_once_with(None)
            assert client._retry_count == 0
    
    @pytest.mark.asyncio
    async def test_handle_reconnection_with_cursor(self, client: BlueskyFirehoseClient) -> None:
        """Test reconnection with cursor."""
        cursor = "test_cursor_456"
        
        with patch.object(client, 'connect_to_firehose') as mock_connect:
            mock_connect.return_value = None
            
            await client.handle_reconnection(cursor=cursor)
            
            mock_connect.assert_called_once_with(cursor)
            assert client._cursor == cursor
    
    @pytest.mark.asyncio
    async def test_handle_reconnection_max_retries(self, client: BlueskyFirehoseClient) -> None:
        """Test reconnection failure after max retries."""
        with patch.object(client, 'connect_to_firehose') as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")
            
            with pytest.raises(AtProtocolError, match="Reconnection failed after 3 attempts"):
                await client.handle_reconnection()
            
            assert mock_connect.call_count == 3
            assert client._retry_count == 3
    
    @pytest.mark.asyncio
    async def test_handle_reconnection_exponential_backoff(self, client: BlueskyFirehoseClient) -> None:
        """Test exponential backoff during reconnection."""
        call_count = 0
        
        async def mock_connect_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Connection failed")
            # Success on third attempt
        
        with patch.object(client, 'connect_to_firehose', side_effect=mock_connect_side_effect):
            with patch('asyncio.sleep') as mock_sleep:
                await client.handle_reconnection()
                
                # Should have slept twice (before 2nd and 3rd attempts)
                assert mock_sleep.call_count == 2
                # Check exponential backoff: 0.2 (2^1 * 0.1), 0.4 (2^2 * 0.1)
                # First attempt doesn't sleep, second attempt sleeps 0.2, third attempt sleeps 0.4
                mock_sleep.assert_any_call(0.2)
                mock_sleep.assert_any_call(0.4)
    
    @pytest.mark.asyncio
    async def test_disconnect(self, client: BlueskyFirehoseClient) -> None:
        """Test disconnection from firehose."""
        # Set up connected state
        client._client = AsyncMock()
        client._is_connected = True
        client._posts_processed = 100
        
        await client.disconnect()
        
        assert client._client is None
        assert not client.is_connected
    
    def test_get_stats(self, client: BlueskyFirehoseClient) -> None:
        """Test statistics retrieval."""
        # Set up some state
        client._is_connected = True
        client._posts_processed = 42
        client._connection_attempts = 2
        client._retry_count = 1
        client._cursor = "test_cursor"
        client._last_message_time = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        
        stats = client.get_stats()
        
        assert stats['is_connected'] is True
        assert stats['posts_processed'] == 42
        assert stats['connection_attempts'] == 2
        assert stats['retry_count'] == 1
        assert stats['current_cursor'] == "test_cursor"
        assert stats['last_message_time'] == "2024-01-15T10:30:00+00:00"
        assert 'correlation_id' in stats
    
    def test_properties(self, client: BlueskyFirehoseClient) -> None:
        """Test client properties."""
        # Test initial state
        assert not client.is_connected
        assert client.current_cursor is None
        assert client.posts_processed == 0
        assert client.last_message_time is None
        
        # Set some state
        client._is_connected = True
        client._cursor = "test_cursor"
        client._posts_processed = 10
        client._last_message_time = datetime.now(timezone.utc)
        
        assert client.is_connected
        assert client.current_cursor == "test_cursor"
        assert client.posts_processed == 10
        assert client.last_message_time is not None


class TestBlueskyFirehoseClientIntegration:
    """Integration tests for BlueskyFirehoseClient with mocked firehose."""
    
    @pytest.fixture
    def client(self) -> BlueskyFirehoseClient:
        """Create client for integration testing."""
        return BlueskyFirehoseClient(max_retries=2, initial_backoff=0.01)
    
    @pytest.mark.asyncio
    async def test_full_message_processing_flow(self, client: BlueskyFirehoseClient) -> None:
        """Test complete message processing flow with mocked firehose."""
        # Create mock message with commit
        mock_message = MagicMock()
        mock_message.seq = 12345
        
        # Create mock parsed message
        mock_parsed = MagicMock()
        mock_commit = MagicMock()
        mock_commit.repo = "did:plc:test123"
        mock_commit.seq = 12345
        
        # Create mock operation
        mock_op = MagicMock()
        mock_op.action = "create"
        mock_op.path = "app.bsky.feed.post/test123"
        
        # Create mock record
        mock_record = MagicMock()
        mock_record.text = "Test post #test @user"
        mock_record.createdAt = "2024-01-15T10:30:00.000Z"
        mock_record.langs = ["en"]
        mock_record.reply = None
        
        mock_op.record = mock_record
        mock_commit.ops = [mock_op]
        mock_parsed.commit = mock_commit
        
        # Mock the firehose client and message parsing
        with patch('bluesky_client.AsyncFirehoseSubscribeReposClient') as mock_client_class:
            with patch('bluesky_client.parse_subscribe_repos_message') as mock_parse:
                mock_firehose_client = AsyncMock()
                mock_client_class.return_value = mock_firehose_client
                
                # Set up async generator for messages
                async def mock_get_messages(cursor=None):
                    yield mock_message
                
                mock_firehose_client.get_messages = mock_get_messages
                mock_parse.return_value = mock_parsed
                
                # Connect and process
                await client.connect_to_firehose()
                
                posts = []
                async for post in client.process_firehose_stream():
                    posts.append(post)
                    break  # Just process one message
                
                assert len(posts) == 1
                post = posts[0]
                assert post.text == "Test post #test @user"
                assert post.author_did == "did:plc:test123"
                assert "test" in post.hashtags
                assert "user" in post.mentions
                assert client.current_cursor == "12345"
                assert client.posts_processed == 1
    
    @pytest.mark.asyncio
    async def test_stream_processing_with_malformed_message(self, client: BlueskyFirehoseClient) -> None:
        """Test stream processing handles malformed messages gracefully."""
        # Create one good message and one bad message
        good_message = MagicMock()
        good_message.seq = 12345
        
        bad_message = MagicMock()
        bad_message.seq = 12346
        
        with patch('bluesky_client.AsyncFirehoseSubscribeReposClient') as mock_client_class:
            with patch('bluesky_client.parse_subscribe_repos_message') as mock_parse:
                mock_firehose_client = AsyncMock()
                mock_client_class.return_value = mock_firehose_client
                
                # Set up async generator with good and bad messages
                async def mock_get_messages(cursor=None):
                    yield good_message
                    yield bad_message
                
                mock_firehose_client.get_messages = mock_get_messages
                
                # First call succeeds, second call fails
                def parse_side_effect(message):
                    if message == good_message:
                        mock_parsed = MagicMock()
                        mock_parsed.commit = None  # No commit, should be skipped
                        return mock_parsed
                    else:
                        raise Exception("Malformed message")
                
                mock_parse.side_effect = parse_side_effect
                
                await client.connect_to_firehose()
                
                # Should process without crashing
                posts = []
                message_count = 0
                async for post in client.process_firehose_stream():
                    posts.append(post)
                    message_count += 1
                    if message_count >= 2:  # Process both messages
                        break
                
                # Should have processed both messages but extracted no posts
                assert len(posts) == 0
                assert client.current_cursor == "12346"  # Updated to last message