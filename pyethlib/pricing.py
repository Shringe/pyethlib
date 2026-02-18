from collections.abc import MutableMapping
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, Iterator

import requests


def truncate_hour(dt: datetime) -> datetime:
    "Truncates a timestamp to the beginning of the hour"
    return dt.replace(minute=0, second=0, microsecond=0, tzinfo=UTC)


def num_hours_between_dates(dt1: datetime, dt2: datetime) -> int:
    return int(abs((dt2 - dt1).total_seconds() // 3600))


@dataclass
class PricingEntry:
    open: float
    closed: float
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
        self, starting_hour: datetime, ending_hour: datetime
    ) -> HourlyPriceHistory:
        """
        Returns a dictionary of hours to PricingEntrys.
        Timezone is UTC.
        """

        starting_hour = truncate_hour(starting_hour)
        ending_hour = truncate_hour(ending_hour)
        requested_limit = num_hours_between_dates(ending_hour, starting_hour)

        # The api doesn't like limits less than one
        if requested_limit == 0:
            only_get_one_entry = True
            true_limit = 1
        else:
            only_get_one_entry = False
            true_limit = requested_limit

        url = "https://min-api.cryptocompare.com/data/v2/histohour"
        pricing_history = HourlyPriceHistory()

        parameters = {
            "fsym": "ETH",
            "tsym": "USD",
            "limit": true_limit,
            "toTs": ending_hour.timestamp(),
            "api_key": self.api_key,
        }

        response = requests.get(url, params=parameters)
        result = response.json()

        for entry in result["Data"]["Data"]:
            dt = datetime.utcfromtimestamp(entry["time"])
            pricing_entry = PricingEntry(
                float(entry["open"]),
                float(entry["close"]),
                float(entry["high"]),
                float(entry["low"]),
            )

            pricing_history[dt] = pricing_entry

            if only_get_one_entry:
                break

        return pricing_history
