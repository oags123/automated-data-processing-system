#shared data classs


import pandas as pd

class SharedDataStore:
    def __init__(self):
        # Structure: storage[data_type][symbol][interval] = DataFrame
        self.storage = {}

    def init(self, data_type, symbol, interval, columns=None):
        """Initialize an empty DataFrame if it doesn't exist."""
        self.storage.setdefault(data_type, {}).setdefault(symbol, {})
        if interval not in self.storage[data_type][symbol]:
            self.storage[data_type][symbol][interval] = pd.DataFrame(columns=columns or [])

    def update(self, data_type, symbol, interval, row_dict):
        """Append a row to the DataFrame."""
        self.init(data_type, symbol, interval, columns=row_dict.keys())
        df = self.storage[data_type][symbol][interval]
        self.storage[data_type][symbol][interval] = pd.concat(
            [df, pd.DataFrame([row_dict])],
            ignore_index=True
        )

    def get(self, data_type, symbol, interval):
        """Retrieve the DataFrame."""
        return self.storage.get(data_type, {}).get(symbol, {}).get(interval)

    def set(self, data_type, symbol, interval, dataframe):
        """Directly set the DataFrame."""
        self.storage.setdefault(data_type, {}).setdefault(symbol, {})[interval] = dataframe

    def exists(self, data_type, symbol, interval):
        """Check if a DataFrame exists."""
        return interval in self.storage.get(data_type, {}).get(symbol, {})



"""

Pure In-Memory `SharedDataStore`

This version is:

* **In-memory only**
* **High-performance**, O(1) access via nested dictionaries
* **No CSV save/load**
* Supports all your `data_types`:

  * `prices`
  * `qr_lines`
  * `lines_properties`
  * `lines_coordinates`
  * `start_time_lower_interval`


## ✅ Example Usage


store = SharedDataStore()

### Initialize:

store.init("prices", "BTC", "5m", columns=["timestamp", "price"])


### Update:

store.update("lines_properties", "ETH", "30m", {
    "timestamp": datetime.now(),
    "slope": 0.015
})


### Retrieve:

df = store.get("qr_lines", "XRP", "1m")
if df is not None:
    print(df.tail())


### Set directly:


store.set("lines_coordinates", "SOL", "4h", pd.DataFrame([...]))


---

## 🧠 Performance Notes

* **In-memory access** is near-instant (`O(1)` dict lookup).
* You can store **hundreds or thousands of DataFrames** with negligible overhead (especially since many will be relatively small).
* You can plug this into any background job, and it'll just work.

---

## ✅ Summary

* **Fast**, **clean**, **scalable**: all in-memory with no unnecessary I/O.
* No lock needed (you already guarantee serial execution via APScheduler).
* Easy to extend if needed later (e.g. add TTL, max length, etc.).


"""