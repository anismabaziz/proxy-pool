import sqlite3
from datetime import datetime


def init_db(db_path: str = "proxies.db") -> None:
  
  conn = sqlite3.connect(db_path)
  cursor = conn.cursor()

  cursor.execute('''
    CREATE TABLE IF NOT EXISTS proxies (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ip TEST NOT NULL,
      port INTEGER NOT NULL,
      protocol TEXT DEFAULT 'http',
      latency_ms INTEGER,
      last_checked DATETIME,
      times_used INTEGER DEFAULT 0,
      UNIQUE(ip, port)
      )
  ''')
  cursor.execute('CREATE INDEX IF NOT EXISTS idx_latency ON proxies(latency_ms)')

  conn.commit()
  conn.close()


def save_working_proxies(proxies_with_latency: list[tuple[str, int]]) -> None:

  conn = sqlite3.connect("proxies.db")
  cursor = conn.cursor()
  now = datetime.now().isoformat()

  for proxy, latency in proxies_with_latency:
    ip, port = proxy.split(":")

    try:
      cursor.execute('''
      INSERT INTO proxies (ip, port, latency_ms, last_checked) VALUES
      (?, ?, ?, ?)
      ON CONFLICT(ip, port) DO UPDATE SET
          latency_ms = EXCLUDED.latency_ms,
          last_checked = EXCLUDED.last_checked
      ''', (ip, int(port), latency, now))
    
    except Exception as e:
      print(f"DB error on {proxy}: {e}")

  conn.commit()
  conn.close()


def get_proxies() -> list[str]:
  """
  Returns every stored proxy as an "ip:port" address
  """
  conn = sqlite3.connect("proxies.db")
  cursor = conn.cursor()

  cursor.execute("SELECT * FROM proxies")
  proxies = cursor.fetchall()


  new_proxies = [f"{ip}:{port}" for _, ip, port, _, latency, *_ in proxies]

  conn.close()
  return new_proxies


def remove_proxies(last_checked: str) -> tuple[bool, int]:
  """
  Removes every proxy that was not touched at last_checked
  """
  conn = sqlite3.connect("proxies.db")
  cursor = conn.cursor()

  cursor.execute('''
  DELETE FROM proxies WHERE last_checked != ?
  ''', 
  (last_checked,)
  )

  conn.commit()
  deleted = cursor.rowcount

  conn.close()

  return (deleted > 0, deleted)


def update_proxies(target: list[tuple[str, int]]) -> tuple[bool, int, str]:
  """
  Updates the proxies with the new latency returns (updated, count, last_checked)
  """
  conn = sqlite3.connect("proxies.db")
  cursor = conn.cursor()

  last_checked = datetime.now().isoformat()
  data = [(latency, last_checked, proxy.split(":")[0], int(proxy.split(":")[1]))  for proxy, latency in target]

  cursor.executemany(
    '''
    UPDATE proxies 
    SET latency_ms = ?,
        last_checked = ?
    WHERE ip = ? AND port = ?
    ''',
    data
  )

  conn.commit()
  updated = cursor.rowcount

  conn.close()

  return (updated > 0, len(target), last_checked)
