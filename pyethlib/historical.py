from dataclasses import dataclass
from pathlib import Path
import sqlite3
from typing import List, Optional


BIGQUERY_MOCK_DATASET = "bigquery-public-data.goog_blockchain_ethereum_goerli_us"
BIGQUERY_REAL_DATASET = "bigquery-public-data.goog_blockchain_ethereum_mainnet_us"


@dataclass
class Query:
    starting_date: Optional[str] = None
    'Format: "YYYY-MM-DD". Will get everything past this date, inclusive'
    ending_date: Optional[str] = None
    'Format: "YYYY-MM-DD". Will get everything up until this date, inclusive'
    limit: Optional[int] = 100
    "The maximum amount of entries to grab"
    dataset: str = BIGQUERY_REAL_DATASET
    "The Google BigQuery ethereum dataset to use"
    table: str = "receipts"
    "The table to get data from. Not recommended to change"
    fields: List[str] | str = "*"
    "The list of fields to grab from the table. Not recommended to change"

    def to_sql(self) -> str:
        if type(self.fields) is str:
            fields = self.fields
        else:
            fields = ", ".join(self.fields)

        parameters: List[str] = []
        parameters.append(f"SELECT {fields}")
        parameters.append(f"FROM `{self.dataset}.{self.table}`")

        if self.starting_date:
            parameters.append(
                f'WHERE block_timestamp >= TIMESTAMP("{self.starting_date}")'
            )

        if self.ending_date:
            # Increment ending_date by one to make date inclusive, rather than exclusive
            date_numbers = self.ending_date.split("-")
            date_numbers[2] = str(int(date_numbers[2]) + 1)
            ending_date_inclusive = "-".join(date_numbers)
            if self.starting_date:
                parameters.append(
                    f'AND block_timestamp < TIMESTAMP("{ending_date_inclusive}")'
                )
            else:
                parameters.append(
                    f'WHERE block_timestamp < TIMESTAMP("{ending_date_inclusive}")'
                )

        if self.limit:
            parameters.append(f"LIMIT {self.limit}")

        return "\n".join(parameters)


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
                status              INTEGER
            )
        """)

    def save_query_results(self, results) -> None:
        "Saves the results from bigquery.Client.query_and_wait() into the database"
        cursor = self.conn.cursor()
        for row in results:
            cursor.execute(
                f"""INSERT OR REPLACE INTO {self.dataset_name} VALUES (
                    :block_hash, :block_number, :block_timestamp, :transaction_hash,
                    :transaction_index, :from_address, :to_address, :contract_address,
                    :cumulative_gas_used, :gas_used, :effective_gas_price,
                    :logs_bloom, :root, :status
                )""",
                dict(row),
            )
