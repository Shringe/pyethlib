import sqlite3
from pathlib import Path
from typing import List, Optional

from google.cloud import bigquery

from pyethlib.historical import Query, ReceiptsEntry
from pyethlib.pricing import PricingData


class MasterClient:
    def __init__(
        self, bigquery_keyfile: str, cryptocompare_keyfile: Optional[str] = None
    ) -> None:
        self._bigquery_keyfile = bigquery_keyfile
        if cryptocompare_keyfile:
            self._cryptocompare_keyfile = cryptocompare_keyfile
        else:
            self._cryptocompare_keyfile = "/dev/null"

        self._bigquery_client = bigquery.Client.from_service_account_json(
            self._bigquery_keyfile
        )
        self._cryptocompare_client = PricingData(Path(self._cryptocompare_keyfile))
        self.dataset: List[ReceiptsEntry] = []

    def fetch_historical_data(self, query: Query) -> None:
        rows = self._bigquery_client.query_and_wait(query.to_sql())
        for row in rows:
            entry = ReceiptsEntry.from_dict(dict(row))
            self.dataset.append(entry)
        self.dataset.sort(key=lambda entry: entry.block_timestamp)

    def fetch_pricing_data(self, padding: int = 0) -> None:
        first = self.dataset[0]
        last = self.dataset[-1]

        start = first.block_timestamp.subtract(hours=padding).round("hour")
        end = last.block_timestamp.add(hours=padding).round("hour")

        price_history = self._cryptocompare_client.get_hourly_pricing(start, end)

        for i, receipt in enumerate(self.dataset):
            try:
                pricing_entry = price_history[receipt.block_timestamp]
            except KeyError:
                raise KeyError(
                    "Failed to fetch pricing data for a specific block. Try increasing the `padding` parameter"
                )

            self.dataset[i].set_pricing(pricing_entry)

    def save_to_sqlite(self, db_path: str) -> None:
        db = Database(path=db_path)
        db.create()
        db.serialize(self.dataset)
        db.close()


class Database:
    def __init__(
        self, dataset_name: str = "pyethlib", path: str = "pyethlib.db"
    ) -> None:
        self.path = Path(path)
        self.dataset_name = dataset_name
        self._open()

    def _open(self) -> None:
        "Opens the database. This is called automatically in the constructor"
        self.conn = sqlite3.connect(self.path)

    def reset(self) -> None:
        "Completely wipes the database and creates a new empty one"
        self.close()
        self.delete()
        self._open()
        self.create()

    def close(self) -> None:
        "Closes the database. Call this once you are done with the data"
        self.conn.commit()
        self.conn.close()

    def delete(self) -> None:
        "Simply deletes the database file"
        self.path.unlink()

    def create(self) -> None:
        "Creates the database"
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.dataset_name} (
                block_hash          TEXT,
                block_number        INTEGER,
                block_timestamp     DATETIME,
                transaction_hash    TEXT PRIMARY KEY,
                transaction_index   INTEGER,
                from_address        TEXT,
                to_address          TEXT,
                contract_address    TEXT,
                cumulative_gas_used INTEGER,
                gas_used            INTEGER,
                effective_gas_price INTEGER,
                logs_bloom          TEXT,
                root                TEXT,
                status              INTEGER,
                pricing_open        REAL,
                pricing_closed      REAL,
                pricing_high        REAL,
                pricing_low         REAL
            )
        """)

    def serialize(self, rows: List[ReceiptsEntry]) -> None:
        sql = f"""
            INSERT OR REPLACE INTO {self.dataset_name} (
                block_hash, block_number, block_timestamp, transaction_hash,
                transaction_index, from_address, to_address, contract_address,
                cumulative_gas_used, gas_used, effective_gas_price, logs_bloom,
                root, status, pricing_open, pricing_closed, pricing_high, pricing_low
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """
        data = [
            (
                row.block_hash,
                row.block_number,
                row.block_timestamp.py_datetime(),
                row.transaction_hash,
                row.transaction_index,
                row.from_address,
                row.to_address,
                row.contract_address,
                row.cumulative_gas_used,
                row.gas_used,
                row.effective_gas_price,
                row.logs_bloom,
                row.root,
                row.status,
                row.pricing_open,
                row.pricing_closed,
                row.pricing_high,
                row.pricing_low,
            )
            for row in rows
        ]

        with self.conn:
            self.conn.executemany(sql, data)
