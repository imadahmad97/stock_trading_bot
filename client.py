from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import BarData
import threading
import time
import datetime
import pandas as pd
from transform_data import transform_data


class IBKRBot(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = []
        self.req_complete = threading.Event()

    def to_dataframe(self):
        import pandas as pd

        df = pd.DataFrame(
            [
                {
                    "date": bar.date,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "barCount": bar.barCount,
                    "wap": bar.wap,  # Use .wap instead of .average as you fixed earlier
                }
                for bar in self.data
            ]
        )
        df["date"] = pd.to_datetime(
            df["date"], format="%Y%m%d %H:%M:%S %Z", errors="coerce"
        )
        df.set_index("date", inplace=True)
        return df

    def nextValidId(self, orderId: int):
        print(f"[{datetime.datetime.now()}] Connected. Requesting data...")
        self.request_data()

    def request_data(self):
        contract = Contract()
        contract.symbol = "SPY"
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        self.reqHistoricalData(
            reqId=1,
            contract=contract,
            endDateTime="",
            durationStr="1 D",
            barSizeSetting="5 mins",
            whatToShow="TRADES",
            useRTH=1,
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[],
        )

    def historicalData(self, reqId: int, bar: BarData):
        self.data.append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print(f"[{datetime.datetime.now()}] Got {len(self.data)} bars.")
        # Run your logic here
        self.run_strategy()
        self.disconnect()
        self.req_complete.set()

    def run_strategy(self):
        df = self.to_dataframe()
        df.to_csv("data/historical_data.csv")  # Save for use in another file
        print("Saved data to CSV.")
        df = transform_data(df)
        df.to_csv("data/historical_with_ema.csv")


def run_once():
    app = IBKRBot()
    app.connect("127.0.0.1", 7496, clientId=1)
    thread = threading.Thread(target=app.run)
    thread.start()
    app.req_complete.wait()
    thread.join()


def wait_until_next_5_min():
    now = datetime.datetime.now()
    # Wait until next 5-minute boundary
    mins_to_wait = 5 - (now.minute % 5)
    next_time = now.replace(second=0, microsecond=0) + datetime.timedelta(
        minutes=mins_to_wait
    )
    sleep_time = (next_time - now).total_seconds()
    print(f"Sleeping for {int(sleep_time)} seconds until {next_time}")
    time.sleep(sleep_time)


if __name__ == "__main__":
    while True:
        wait_until_next_5_min()
        run_once()
