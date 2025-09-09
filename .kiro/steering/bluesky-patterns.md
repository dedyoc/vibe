---
inclusion: fileMatch
fileMatchPattern: '*bluesky*|*firehose*|*atproto*|*bsky*'
---

# Bluesky/ATProto Streaming Patterns

## Firehose Integration
When working with Bluesky firehose data:

### Core Components
- Use `AsyncFirehoseSubscribeReposClient` for async streaming
- Handle `ComAtprotoSyncSubscribeRepos.Commit` messages
- Process CAR (Content Addressable aRchive) blocks for record data
- Track cursor positions for resumable streams

### Record Types of Interest
Focus on these ATProto record types:
```python
_INTERESTED_RECORDS = {
    models.ids.AppBskyFeedPost: models.AppBskyFeedPost,      # Posts/tweets
    models.ids.AppBskyFeedLike: models.AppBskyFeedLike,      # Likes
    models.ids.AppBskyGraphFollow: models.AppBskyGraphFollow, # Follows
}
```

### Message Processing Pattern
```python
async def process_commit(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> None:
    """Process a firehose commit message."""
    if not commit.blocks:
        return
    
    ops = _get_ops_by_type(commit)
    
    # Process created posts
    for created_post in ops[models.ids.AppBskyFeedPost]['created']:
        await process_post(created_post)
    
    # Update cursor periodically for resumability
    if commit.seq % 20 == 0:
        client.update_params(
            models.ComAtprotoSyncSubscribeRepos.Params(cursor=commit.seq)
        )
```

### Performance Monitoring
- Implement events-per-second monitoring for network load
- Track processing latency and backlog
- Monitor memory usage for sliding windows
- Handle graceful shutdown with signal handlers

### Error Handling
- Handle network disconnections and reconnections
- Validate CAR block data before processing
- Skip malformed or incomplete records gracefully
- Implement exponential backoff for connection retries

### Data Extraction
- Extract text content from posts: `record.text`
- Parse timestamps: `record.created_at`
- Track authors: `commit.repo` (DID)
- Handle inline text formatting (newlines, mentions, links)

## Trend Detection on Social Data
- Monitor hashtag frequency over time windows
- Track mention patterns and viral content
- Analyze sentiment trends in post content
- Detect emerging topics through keyword clustering