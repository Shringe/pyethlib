from dataclasses import dataclass
from typing import List, Optional

from whenever import Instant, Date
from pyethlib.pricing import PricingEntry


BIGQUERY_MOCK_DATASET = "bigquery-public-data.goog_blockchain_ethereum_goerli_us"
BIGQUERY_REAL_DATASET = "bigquery-public-data.goog_blockchain_ethereum_mainnet_us"


@dataclass
class Query:
    """
    Dataclass to format and retrieve SQL data from Google's BigQuery datasets.
    Fields prefixed with with _underscore are not recommended to change.
    """

    start: Optional[Instant] = None
    "Will get everything past this instant"
    end: Optional[Instant] = None
    "Will get everything up until this instant"
    max_entries: Optional[int] = 1000
    "The maximum amount of total entries to grab"
    entries_per_hour: Optional[int] = 10
    "The maximum amount of entries to grab at each hour"
    dataset: str = BIGQUERY_REAL_DATASET
    "The Google BigQuery ethereum dataset to use"
    _table: str = "receipts"
    "The table to get data from. Not recommended to change"
    _fields: List[str] | str = "*"
    "The list of fields to grab from the table. Not recommended to change"

    def to_sql(self) -> str:
        if type(self._fields) is str:
            fields = self._fields
        else:
            fields = ", ".join(self._fields)

        parameters: List[str] = []
        parameters.append(f"SELECT {fields}")
        parameters.append(f"FROM `{self.dataset}.{self._table}`")

        where_clauses: List[str] = []

        if self.start:
            where_clauses.append(
                f'block_timestamp >= TIMESTAMP("{self.start.round("microsecond").format_common_iso()}")'
            )

        if self.end:
            where_clauses.append(
                f'block_timestamp < TIMESTAMP("{self.end.round("microsecond").format_common_iso()}")'
            )

        if where_clauses:
            parameters.append("WHERE " + "\nAND ".join(where_clauses))

        if self.entries_per_hour:
            parameters.append(
                "QUALIFY ROW_NUMBER() OVER "
                "(PARTITION BY TIMESTAMP_TRUNC(block_timestamp, HOUR) "
                "ORDER BY block_timestamp) "
                f"<= {self.entries_per_hour}"
            )

        if self.max_entries:
            parameters.append(f"LIMIT {self.max_entries}")

        return "\n".join(parameters)


@dataclass
class ReceiptsEntry:
    "Receipts table entry, with fields for tacking on pricing data"

    block_hash: str
    block_number: int
    block_timestamp: Instant
    transaction_hash: str
    transaction_index: int
    from_address: str
    to_address: Optional[str]
    contract_address: Optional[str]
    cumulative_gas_used: int
    gas_used: int
    effective_gas_price: int
    logs_bloom: str
    root: Optional[str]
    status: Optional[int]

    pricing_open: Optional[float] = None
    pricing_closed: Optional[float] = None
    pricing_high: Optional[float] = None
    pricing_low: Optional[float] = None

    def set_pricing(self, pricing: PricingEntry) -> None:
        self.pricing_open = pricing.open
        self.pricing_closed = pricing.closed
        self.pricing_high = pricing.high
        self.pricing_low = pricing.low

    def get_pricing(self) -> PricingEntry:
        return PricingEntry(
            self.pricing_open,  # pyright: ignore[reportArgumentType]
            self.pricing_closed,  # pyright: ignore[reportArgumentType]
            self.pricing_high,  # pyright: ignore[reportArgumentType]
            self.pricing_low,  # pyright: ignore[reportArgumentType]
        )

    @classmethod
    def from_dict(cls, d: dict) -> "ReceiptsEntry":
        return cls(
            block_hash=d["block_hash"],
            block_number=d["block_number"],
            block_timestamp=Instant.from_py_datetime(d["block_timestamp"]),
            transaction_hash=d["transaction_hash"],
            transaction_index=d["transaction_index"],
            from_address=d["from_address"],
            to_address=d.get("to_address"),
            contract_address=d.get("contract_address"),
            cumulative_gas_used=d["cumulative_gas_used"],
            gas_used=d["gas_used"],
            effective_gas_price=d["effective_gas_price"],
            logs_bloom=d["logs_bloom"],
            root=d.get("root"),
            status=d.get("status"),
        )
