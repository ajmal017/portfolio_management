import concurrent.futures
import datetime
from csv import DictWriter

from _pytest import logging

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



    def __init__(self, tickers, durationString: str,
                 barSizeSetting: str,
                 folder_path: str,
                 forMomentumStrategy = False,
                 first_n_stocks = None,
                 time_from_today = None):


        EClient.__init__(self,self)

        if first_n_stocks is None:
            self.tickers = tickers
        else:
            self.tickers = tickers[:first_n_stocks]

        self.date = datetime.date.today().strftime("%d%m%Y")
        self.orderId = 1
        self.forMomentumStrategy = forMomentumStrategy
        self.count = 0
        self.folder_path = folder_path
        self.time_from_today = time_from_today
        self.first_n_stocks = first_n_stocks
        self.durationString = durationString
        self.barSizeSetting = barSizeSetting



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

        file_path = "{}_{}_history.csv".format(self.tickers[self.count], self.date)
        path = os.path.join(self.folder_path, file_path)
        row = {"date": bar.date, "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close, "volume": bar.volume}

        self.append_dict_as_row(path,row,self.field_names)

    def historicalDataEnd(self, reqId:int, start:str, end:str):
        self.count += 1
        if self.count == len(self.tickers):
            self.done = True
            self.disconnect()

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)

        print("setting nextValidOrderId: %d", orderId)

        self.nextValidOrderId = orderId
        print("NextValidId:", orderId)
    # ! [nextvalidid]

        # we can start now
        self.start()
        #self.check_files(paths= paths)

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
            filepath = "{}_{}_history.csv".format(ticker, self.date)
            path = os.path.join(folder_path, filepath)
            #path = "/Users/alperoner/PycharmProjects/PMP/History/{}_history.csv".format(ticker)
            levels = ["date", "open", "high", "low", "close", "volume"]
            df = pd.DataFrame(columns=levels)
            df.to_csv(path,index=False)
            csv_paths.append(path)
        return csv_paths

    def check_files(self, paths):

        paths_filled = False 
        
        for path in paths:
            data = pd.read_csv(path)
            if data is None:
                raise("Files not filled")
            elif len(data) == 0:
                raise ("Files not filled")

        if paths_filled:
            print("Mission Completed with Succes!!")


    def start(self):


        paths = self.create_csv_files(self.tickers, folder_path=self.folder_path)
        contracts = self.getContracts(self.tickers)

        if self.time_from_today is None:
            queryTime = ""
        else:
            queryTime = (datetime.datetime.today() - datetime.timedelta(days=self.time_from_today)).strftime(
                "%Y%m%d %H:%M:%S")

        for contract in contracts:
            self.reqHistoricalData(self.nextOrderId(), contract,
                                   queryTime, self.durationString,
                                   self.barSizeSetting, "MIDPOINT",
                                   1, 1, False, [])
            # endDateTime, The request's end date and time (the empty string indicates current present moment).
            # durationString, The amount of time (or Valid Duration String units) to go back from the request's given end date and time.
            # barSizeSetting, The data's granularity or Valid Bar Sizes

        return paths


#####
#####
        ##Questionable if statement might be unnecessary


def main():

    data = pd.read_csv("/Users/alperoner/PycharmProjects/PMP/sp500.csv")
    tickers = data.ticker.values[10:20]
    app = HistoricalData(tickers= tickers,
                         durationString= "30 Y", barSizeSetting = "1 month",
                         folder_path= "/Users/alperoner/PycharmProjects/PMP/History",
                         forMomentumStrategy=False)

    app.connect("127.0.0.1", 7497, 1)
    app.run()


def momentum():

    tickers_path = "/Users/alperoner/PycharmProjects/PMP/IntradayMomentum/Scanner/{}_scanner.csv".format(datetime.date.today().strftime("%d%m%Y"))
    data = pd.read_csv(tickers_path)
    tickers = data.ticker.values
    app = HistoricalData(tickers= tickers,
                         durationString= "1 D", barSizeSetting = "1 day",
                         folder_path="/Users/alperoner/PycharmProjects/PMP/IntradayMomentum/DailyPrices",
                         first_n_stocks= 5,
                         forMomentumStrategy=True)
    app.connect("127.0.0.1", 7497, 1)
    app.run()


momentum()






#if __name__ == "__main__":

    ##If threading goes wrong
    #paths = main(tickers= tickers, durationString="30 Y", barSizeSetting="1 month")
    #view(paths)

    ## We can turn this into pool with mapping
    #with concurrent.futures.ThreadPoolExecutor() as executor:
    #    args = ["30 Y", "1 month"]
   #     future = executor.submit(lambda p: main(*p), args)
  #      return_value = future.result()
        #view(return_value)


#if __name__ == "__main__":
 #   with concurrent.futures.ThreadPoolExecutor() as executor:
  #      args = ["1 D", "1 day", "/Users/alperoner/PycharmProjects/PMP/IntradayMomentum/DailyPrices", True, 5]
   #     future = executor.submit(lambda p: main(*p), args)
    #    return_value = future.result()

    #x = threading.Thread(target=main, args=("30 Y","1 month"))
    #x.start()

    #print(x.join())
