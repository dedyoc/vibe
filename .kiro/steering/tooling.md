---
inclusion: manual
---

# Development Tooling

## Package Management with uv
- Use `uv` for fast Python package management
- Create and maintain `pyproject.toml` for dependencies
- Use `uv sync` to install dependencies
- Use `uv add <package>` to add new dependencies

## Code Quality with ruff
- Use ruff for both linting and formatting
- Configuration in `pyproject.toml`:
```toml
[tool.ruff]
line-length = 88
target-version = "py39"

[tool.ruff.lint]
select = ["E", "F", "W", "I", "N", "UP"]
ignore = ["E501"]  # Line too long (handled by formatter)
```

## Docker Compose Development
```bash
# Start the entire streaming pipeline
docker-compose up -d

# View logs for specific service
docker-compose logs -f bluesky-producer

# Scale Flink task managers
docker-compose up -d --scale flink-taskmanager=3

# Stop all services
docker-compose down

# Rebuild after code changes
docker-compose build && docker-compose up -d
```

## Common Development Commands
```bash
# Python environment setup
uv sync

# Code quality
ruff check .
ruff format .

# Testing
uv run pytest

# Dependencies
uv add kafka-python pyflink
uv add --dev pytest ruff

# Kafka utilities (when containers are running)
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
docker-compose exec kafka kafka-console-consumer --topic bluesky-posts --bootstrap-server localhost:9092
```

## Git Workflow
- Create feature branches: `git checkout -b feat/kafka-integration`
- Test with Docker Compose locally
- Merge to dev branch for integration testing
- Deploy from main branch
- Use descriptive commit messages focusing on the "why"