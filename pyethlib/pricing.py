from collections.abc import MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator

import requests
from whenever import Instant


@dataclass
class PricingEntry:
    open: float
    closed: float
    high: float
    low: float


class HourlyPriceHistory(MutableMapping):
    """A dictionary of hour timestamps mapped to price histories"""

    def __init__(self) -> None:
        self._history: Dict[Instant, PricingEntry] = {}

    def __setitem__(self, dt: Instant, entry: PricingEntry) -> None:
        self._history[dt.round("hour")] = entry

    def __getitem__(self, dt: Instant) -> PricingEntry:
        return self._history[dt.round("hour")]

    def __delitem__(self, dt: Instant) -> None:
        del self._history[dt.round("hour")]

    def __iter__(self) -> Iterator[Instant]:
        return self._history.__iter__()

    def __len__(self) -> int:
        return self._history.__len__()

    def __str__(self) -> str:
        return self._history.__str__()


class PricingData:
    def __init__(self, keyfile: Path) -> None:
        self.api_key = keyfile.read_text()

    def get_hourly_pricing(
        self, starting_hour: Instant, ending_hour: Instant
    ) -> HourlyPriceHistory:
        """
        Returns a dictionary of hours to PricingEntrys.
        Timezone is UTC.
        """

        starting_hour = starting_hour.round("hour")
        ending_hour = ending_hour.round("hour")
        requested_limit = int((ending_hour - starting_hour).in_hours())

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
            dt = Instant.from_timestamp(entry["time"])
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
