import os
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from twelvedata import TDClient
from dotenv import load_dotenv

load_dotenv()

DB_TABLE = "crypto_ticks"
DB_COLUMNS = ["time", "symbol", "price", "day_volume"]
MAX_BATCH_SIZE = 10


class WebsocketPipeline:
    def __init__(self, conn):
        self.conn = conn
        self.current_batch = []
        self.insert_counter = 0

    def _insert_values(self, data):
        with self.conn.cursor() as cursor:
            sql = f"""
                INSERT INTO {DB_TABLE} ({",".join(DB_COLUMNS)})
                VALUES %s;
            """
            execute_values(cursor, sql, data)

        self.conn.commit()
        print(f"Inserted {len(data)} rows into Tiger Cloud.")

    def _on_event(self, event):
        if event.get("event") != "price":
            print("WebSocket event:", event)
            return

        timestamp = datetime.fromtimestamp(
            event["timestamp"], tz=timezone.utc
        )

        row = (
            timestamp,
            event["symbol"],
            event["price"],
            event.get("day_volume"),
        )

        self.current_batch.append(row)

        print(
            f"{event['symbol']}: "
            f"{event['price']} | "
            f"batch size: {len(self.current_batch)}"
        )

        if len(self.current_batch) >= MAX_BATCH_SIZE:
            self._insert_values(self.current_batch)
            self.insert_counter += 1
            print(f"Batch insert #{self.insert_counter}")
            self.current_batch = []

    def start(self, symbols):
        api_key = os.environ["TWELVE_DATA_API_KEY"]

        td = TDClient(apikey=api_key)

        ws = td.websocket(on_event=self._on_event)
        ws.subscribe(symbols)
        ws.connect()

        try:
            while True:
                ws.heartbeat()
                time.sleep(10)
        except KeyboardInterrupt:
            print("\nStopping pipeline...")
        finally:
            ws.disconnect()


def create_connection():
    return psycopg2.connect(
        database=os.environ["TIGER_DB_NAME"],
        host=os.environ["TIGER_DB_HOST"],
        user=os.environ["TIGER_DB_USER"],
        password=os.environ["TIGER_DB_PASSWORD"],
        port=os.environ["TIGER_DB_PORT"],
        sslmode="require",
    )


if __name__ == "__main__":
    connection = create_connection()
    pipeline = WebsocketPipeline(connection)

    pipeline.start(
        symbols=[
            "BTC/USD",
            "ETH/USD",
        ]
    )