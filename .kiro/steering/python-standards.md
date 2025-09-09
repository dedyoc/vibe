# Python Coding Standards

## Code Style
- Follow PEP 8 guidelines but keep it practical, not overly strict
- Use ruff for automatic formatting and linting
- Maintain consistent indentation (4 spaces)

## Type Hints
- **Always include type hints** for function parameters and return values
- Use modern typing syntax (Python 3.9+ style when possible)
- Import types from `typing` module as needed
- Example:
```python
from typing import List, Dict, Optional
from datetime import datetime

def process_ticker_data(
    tickers: List[str], 
    window_size: int,
    start_time: Optional[datetime] = None
) -> Dict[str, float]:
    pass
```

## Function and Variable Naming
- Use descriptive names that clearly indicate purpose
- Functions: `snake_case` with verb-noun pattern (`process_data`, `calculate_trend`)
- Variables: `snake_case` with descriptive nouns (`ticker_prices`, `window_data`)
- Constants: `UPPER_SNAKE_CASE` (`MAX_WINDOW_SIZE`, `DEFAULT_TIMEOUT`)
- Classes: `PascalCase` (`TrendDetector`, `DataProcessor`)

## Documentation
- Include docstrings for all public functions and classes
- Use Google-style docstrings for consistency
- Document complex algorithms and business logic inline

## Error Handling
- Use specific exception types rather than generic Exception
- Implement proper logging for debugging streaming issues
- Handle edge cases in data processing gracefully