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

    starting_date: Optional[Date] = None
    "Will get everything past this date, inclusive"
    ending_date: Optional[Date] = None
    "Will get everything up until this date, inclusive"
    limit: Optional[int] = 100
    "The maximum amount of entries to grab"
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

        if self.starting_date:
            parameters.append(
                f'WHERE block_timestamp >= TIMESTAMP("{self.starting_date.format_common_iso()}")'
            )

        if self.ending_date:
            # Increment ending_date by one to make date inclusive, rather than exclusive
            ending_date_inclusive = self.ending_date.add(days=1)
            if self.starting_date:
                parameters.append(
                    f'AND block_timestamp < TIMESTAMP("{ending_date_inclusive.format_common_iso()}")'
                )
            else:
                parameters.append(
                    f'WHERE block_timestamp < TIMESTAMP("{ending_date_inclusive.format_common_iso()}")'
                )

        if self.limit:
            parameters.append(f"LIMIT {self.limit}")

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
