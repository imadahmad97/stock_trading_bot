from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import BarData
import threading
import time


class IBKRBot(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = []

    def nextValidId(self, orderId: int):
        print(f"Connected. Next valid order ID: {orderId}")
        self.request_data()

    def request_data(self):
        contract = Contract()
        contract.symbol = "SPY"
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        # Request 5-minute bars, 1 day back
        self.reqHistoricalData(
            reqId=1,
            contract=contract,
            endDateTime="",  # now
            durationStr="1 D",
            barSizeSetting="5 mins",
            whatToShow="TRADES",
            useRTH=1,
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[],
        )

    def historicalData(self, reqId: int, bar: BarData):
        print(
            f"{bar.date} | Open: {bar.open} | High: {bar.high} | Low: {bar.low} | Close: {bar.close} | Volume: {bar.volume}"
        )
        self.data.append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print("Done receiving historical data")
        self.disconnect()


def run_bot():
    app = IBKRBot()
    app.connect("127.0.0.1", 7496, clientId=1)

    # run() is blocking, so we use a thread
    thread = threading.Thread(target=app.run)
    thread.start()

    # give it some time to fetch data
    time.sleep(10)


if __name__ == "__main__":
    run_bot()
