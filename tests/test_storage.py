from pathlib import Path

import pytest
from src.storage import get_proxies, init_db, save_working_proxies


@pytest.fixture
def pool(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    init_db()
    return tmp_path / "proxies.db"


def test_saved_proxies_come_back_as_addresses(pool: Path) -> None:
    save_working_proxies([("1.2.3.4:8080", 120), ("5.6.7.8:3128", 340)])

    assert sorted(get_proxies()) == ["1.2.3.4:8080", "5.6.7.8:3128"]


def test_saving_the_same_proxy_twice_does_not_duplicate_it(pool: Path) -> None:
    save_working_proxies([("1.2.3.4:8080", 120)])
    save_working_proxies([("1.2.3.4:8080", 90)])

    assert get_proxies() == ["1.2.3.4:8080"]
