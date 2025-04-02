from ibapi.client import EClient  # type: ignore
from ibapi.wrapper import EWrapper  # type: ignore
from ibapi.contract import Contract  # type: ignore
from ibapi.common import BarData  # type: ignore
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

    def to_dataframe(self) -> pd.DataFrame:

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
                    "wap": bar.wap,
                }
                for bar in self.data
            ]
        )
        df["date"] = pd.to_datetime(
            df["date"], format="%Y%m%d %H:%M:%S %Z", errors="coerce"
        )
        df.set_index("date", inplace=True)
        return df

    def nextValidId(self, orderId: int) -> None:
        print(f"[{datetime.datetime.now()}] Connected. Requesting data...")
        self.request_data()

    def request_data(self) -> None:
        contract = Contract()
        contract.symbol = "SOFI"
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

    def historicalData(self, reqId: int, bar: BarData) -> None:
        self.data.append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        # Run your logic here
        self.run_strategy()
        self.disconnect()
        self.req_complete.set()

    def run_strategy(self) -> None:
        df = self.to_dataframe()
        df.to_csv("data/historical_data.csv")
        print("Saved data to CSV.")
        df = transform_data("data/historical_data.csv")
        df.to_csv("data/historical_with_indicators.csv")


def run_once() -> None:
    app = IBKRBot()
    app.connect("127.0.0.1", 7496, clientId=1)
    thread = threading.Thread(target=app.run)
    thread.start()
    app.req_complete.wait()
    thread.join()


def wait_until_next_5_min() -> None:
    now = datetime.datetime.now()
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
