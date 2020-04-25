import datetime
from csv import DictWriter
from ibapi.client import EClient, TickerId, TickAttrib, BarData
from ibapi.wrapper import EWrapper, TickType
from ibapi.contract import Contract
import pandas as pd
from ibapi.ticktype import TickTypeEnum
import time
import threading

class HistoricalData(EWrapper, EClient):

    symbols = []
    field_names = ["Date", "Open", "High", "Low", "Close", "Volume"]
    count = 0

    def __init__(self):
        EClient.__init__(self,self)
        self.nextorderId = 1


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
        path = "/Users/alperoner/PycharmProjects/PMP/History/{}_history.csv".format(self.symbols[self.count])
        row = {"Date": bar.date, "Open": bar.open, "High": bar.high, "Low": bar.low, "Close": bar.close, "Volume": bar.volume}
        self.append_dict_as_row(path,row,self.field_names)

    def historicalDataEnd(self, reqId:int, start:str, end:str):
        self.count += 1
        if self.count == len(self.symbols):
            self.done = True


    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextorderId = orderId + 1
        #print('The next valid order id is: ', self.nextorderId)


def getContracts(symbols: [str]) -> [Contract]:

    contract_list = []

    for symbol in symbols:
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract_list.append(contract)

    return contract_list

def create_csv_files(symbols: [str]) -> [str]:
    csv_paths = []
    for symbol in symbols:
        path = "/Users/alperoner/PycharmProjects/PMP/History/{}_history.csv".format(symbol)
        levels = ["Date", "Open", "High", "Low", "Close", "Volume"]
        df = pd.DataFrame(columns=levels)
        df.to_csv(path,index=False)
        csv_paths.append(path)
    return csv_paths


def main(symbols, durationString, barSizeSetting, time_from_today = None):

    app = HistoricalData()
    app.connect("127.0.0.1", 7497, 1)
    app.symbols = symbols
    paths = create_csv_files(symbols)
    contracts = getContracts(symbols)

    if time_from_today is None:
        queryTime = ""
    else:
        queryTime = (datetime.datetime.today() - datetime.timedelta(days=180)).strftime("%Y%m%d %H:%M:%S")

    for contract in contracts:
        app.reqHistoricalData(app.nextorderId, contract, queryTime, durationString, barSizeSetting, "MIDPOINT", 1, 1, False, [])
        #endDateTime, The request's end date and time (the empty string indicates current present moment).
        #durationString, The amount of time (or Valid Duration String units) to go back from the request's given end date and time.
        #barSizeSetting, The data's granularity or Valid Bar Sizes

        app.nextValidId(app.nextorderId)

    app.run()
    app.disconnect()

    return paths


def view(paths):
    for path in paths:
        print(path)
        data = pd.read_csv(path)
        print(data.head())

if __name__ == "__main__":
    paths = main(durationString="5 Y", barSizeSetting="1 month")
    view(paths)



