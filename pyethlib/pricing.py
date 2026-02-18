from collections.abc import MutableMapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator

import requests


def truncate_hour(dt: datetime) -> datetime:
    "Truncates a timestamp to the beginning of the hour"
    return dt.replace(minute=0, second=0, microsecond=0)


@dataclass
class PricingEntry:
    open: float
    close: float
    high: float
    low: float


class HourlyPriceHistory(MutableMapping):
    """A dictionary of hour timestamps mapped to price histories"""

    def __init__(self) -> None:
        self._history: Dict[datetime, PricingEntry] = {}

    def __setitem__(self, dt: datetime, entry: PricingEntry) -> None:
        self._history[truncate_hour(dt)] = entry

    def __getitem__(self, dt: datetime) -> PricingEntry:
        return self._history[truncate_hour(dt)]

    def __delitem__(self, dt: datetime) -> None:
        del self._history[truncate_hour(dt)]

    def __iter__(self) -> Iterator[datetime]:
        return self._history.__iter__()

    def __len__(self) -> int:
        return self._history.__len__()

    def __str__(self) -> str:
        return self._history.__str__()


class PricingData:
    def __init__(self, keyfile: Path) -> None:
        self.api_key = keyfile.read_text()

    def get_hourly_pricing(
        self, limit: int, final_hour: datetime
    ) -> HourlyPriceHistory:
        """
        Returns a dictionary of hours to PricingEntrys.
        Limit is the number of hours to get prior to and including final_hour.
        """

        url = "https://min-api.cryptocompare.com/data/v2/histohour"
        pricing_history = HourlyPriceHistory()

        parameters = {
            "fsym": "ETH",
            "tsym": "USD",
            "limit": limit,
            "toTs": truncate_hour(final_hour).timestamp(),
            "api_key": self.api_key,
        }

        response = requests.get(url, params=parameters)
        result = response.json()

        for raw in result["Data"]["Data"]:
            dt = datetime.utcfromtimestamp(raw["time"])
            pricing_entry = PricingEntry(
                float(raw["open"]),
                float(raw["close"]),
                float(raw["high"]),
                float(raw["low"]),
            )

            pricing_history[dt] = pricing_entry

        return pricing_history
