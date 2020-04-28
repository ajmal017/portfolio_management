import concurrent.futures
import datetime
from csv import DictWriter
from ibapi.client import EClient, TickerId, TickAttrib, BarData
from ibapi.wrapper import EWrapper, TickType
from ibapi.contract import Contract
import pandas as pd
from ibapi.ticktype import TickTypeEnum
import time
import threading
import os

class HistoricalData(EWrapper, EClient):

    tickers = []
    field_names = ["date", "open", "high", "low", "close", "volume"]



    def __init__(self, folder_path: str):
        EClient.__init__(self,self)
        self.date = datetime.date.today().strftime("%d%m%Y")
        self.orderId = 1
        self.forMomentumStrategy = False
        self.count = 0
        self.folder_path = folder_path



    def append_dict_as_row(self,file_name, dict_of_elem, field_names):
        # Open file in append mode
        with open(file_name, 'a+', newline='') as write_obj:
            # Create a writer object from csv module
            dict_writer = DictWriter(write_obj, fieldnames=field_names)
            # Add dictionary as wor in the csv
            dict_writer.writerow(dict_of_elem)

    def error(self, reqId:TickerId, errorCode:int, errorString:str):
        print("Error: ", reqId, " ", errorCode, " ", errorString)

    def historicalData(self, reqId: int, bar: BarData):
        file_path = "{}_history.csv".format(self.tickers[self.count])
        path = os.path.join(self.folder_path, file_path)
        row = {"date": bar.date, "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close, "volume": bar.volume}

        self.append_dict_as_row(path,row,self.field_names)

    def historicalDataEnd(self, reqId:int, start:str, end:str):
        self.count += 1
        if self.count == len(self.tickers):
            self.done = True


    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.orderId = orderId + 1
        return self.orderId
        #print('The next valid order id is: ', self.nextorderId)


    def getContracts(self, tickers: [str]) -> [Contract]:

        contract_list = []

        for ticker in tickers:
            contract = Contract()
            contract.symbol = ticker
            contract.secType = "STK"
            contract.exchange = "SMART"
            contract.currency = "USD"
            contract_list.append(contract)

        return contract_list

    def create_csv_files(self, tickers: [str], folder_path: str) -> [str]:
        csv_paths = []
        for ticker in tickers:
            filepath = "{}_history.csv".format(ticker)
            path = os.path.join(folder_path, filepath)
            #path = "/Users/alperoner/PycharmProjects/PMP/History/{}_history.csv".format(ticker)
            levels = ["date", "open", "high", "low", "close", "volume"]
            df = pd.DataFrame(columns=levels)
            df.to_csv(path,index=False)
            csv_paths.append(path)
        return csv_paths


def main(durationString:str, barSizeSetting:str,
         folder_path: str= "/Users/alperoner/PycharmProjects/PMP/History",
         forMomentum = False, time_from_today = None):


    if forMomentum:
        tickers_path = "/Users/alperoner/PycharmProjects/PMP/IntradayMomentum/Scanner/{}_scanner.csv".format(datetime.date.today().strftime("%d%m%Y"))
        data = pd.read_csv(tickers_path)
        tickers = data.ticker.values[0:5]
    else:
        data = pd.read_csv("/Users/alperoner/PycharmProjects/PMP/sp500.csv")
        tickers = data.ticker.values[10:20]


    app = HistoricalData(folder_path=folder_path)
    app.connect("127.0.0.1", 7497, 1)
    app.tickers = tickers
    paths = app.create_csv_files(tickers, folder_path= folder_path)
    contracts = app.getContracts(tickers)

    if time_from_today is None:
        queryTime = ""
    else:
        queryTime = (datetime.datetime.today() - datetime.timedelta(days=180)).strftime("%Y%m%d %H:%M:%S")


    for contract in contracts:
        app.reqHistoricalData(app.orderId, contract, queryTime, durationString, barSizeSetting, "MIDPOINT", 1, 1, False, [])
        #endDateTime, The request's end date and time (the empty string indicates current present moment).
        #durationString, The amount of time (or Valid Duration String units) to go back from the request's given end date and time.
        #barSizeSetting, The data's granularity or Valid Bar Sizes
        app.nextValidId(app.orderId)

    app.run()
    app.disconnect()

    return paths


def view(paths):
    for path in paths:
        print(path)
        data = pd.read_csv(path)
        print(data.head())


if __name__ == "__main__":

    ##If threading goes wrong
    #paths = main(tickers= tickers, durationString="30 Y", barSizeSetting="1 month")
    #view(paths)

    ## We can turn this into pool with mapping
    with concurrent.futures.ThreadPoolExecutor() as executor:
        args = ["30 Y", "1 month"]
        future = executor.submit(lambda p: main(*p), args)
        return_value = future.result()
        #view(return_value)


if __name__ == "__main__":
    with concurrent.futures.ThreadPoolExecutor() as executor:
        args = ["1 D", "1 day", "/Users/alperoner/PycharmProjects/PMP/IntradayMomentum/DailyPrices", True]
        future = executor.submit(lambda p: main(*p), args)
        return_value = future.result()
    #x = threading.Thread(target=main, args=("30 Y","1 month"))
    #x.start()

    #print(x.join())
