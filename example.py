from whenever import Instant

from pyethlib import MasterClient
from pyethlib.historical import Query

now = Instant.now()
query = Query()
query.start = now.subtract(hours=24 * 7)
query.end = now
query.max_entries = 10000
query.entries_per_hour = 10

mc = MasterClient("./pyethlib-************.json", "./cryptocompare_keyfile.txt")

mc.fetch_historical_data(query)
mc.fetch_pricing_data()
mc.save_to_sqlite("example.db")
