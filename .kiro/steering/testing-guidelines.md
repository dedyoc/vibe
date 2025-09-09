---
inclusion: fileMatch
fileMatchPattern: '*test*|test_*'
---

# Testing Guidelines for Streaming Pipeline

## Test Structure
- Use pytest as the testing framework
- Organize tests in `tests/` directory mirroring source structure
- Name test files with `test_` prefix

## Testing Streaming Components
### Unit Tests
- Mock external data sources and sinks
- Test sliding window logic with controlled time sequences
- Verify trend detection algorithms with known datasets
- Test edge cases (empty windows, single data points, etc.)

### Integration Tests
- Test end-to-end data flow with small datasets
- Verify proper handling of data format variations
- Test error scenarios and recovery mechanisms

## Test Data Patterns
```python
import pytest
from datetime import datetime, timedelta
from typing import List, Tuple

@pytest.fixture
def sample_ticker_data() -> List[Tuple[datetime, str, float]]:
    """Generate sample ticker data for testing."""
    base_time = datetime.now()
    return [
        (base_time + timedelta(seconds=i), "AAPL", 150.0 + i * 0.1)
        for i in range(100)
    ]

def test_sliding_window_basic_functionality(sample_ticker_data):
    # Test implementation here
    pass
```

## Performance Testing
- Include benchmarks for critical path operations
- Test memory usage with large datasets
- Verify processing latency meets requirements
- Test system behavior under load