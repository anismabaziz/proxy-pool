from pathlib import Path

import pytest
from src.storage import init_db


@pytest.fixture
def pool(tmp_path: Path) -> Path:
    """A pool of the current shape, in a database of its own"""

    db_path = tmp_path / "pool.db"
    init_db(str(db_path))
    return db_path
