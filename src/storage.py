import sqlite3
from datetime import datetime
from typing import List, Tuple


def init_db(db_path="proxies.db"):
  
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


def save_working_proxies(proxies_with_latency: List[Tuple[str, int]]):

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