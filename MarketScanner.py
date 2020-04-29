from csv import DictWriter

from ibapi.common import TickerId, TickAttrib, BarData
from ibapi.client import EClient ##Outgoing messages
from ibapi.scanner import ScannerSubscription, ScanData
from ibapi.tag_value import TagValue
from ibapi.wrapper import EWrapper ##Incoming messages
from ibapi.contract import Contract, ContractDetails
import pandas as pd
import datetime
import schedule
import time

class TestApp(EWrapper,EClient):

    def __init__(self, subscription: ScannerSubscription): #ticker, secType,exchange,currency,primaryExchange):

        EClient.__init__(self,self)
        self.date = datetime.date.today().strftime("%d%m%Y")
        self.data = pd.DataFrame(columns=["rank", "ticker", "sec_type"])
        self.subscription = subscription

    def create_csv_files(self):
        path = "/Users/alperoner/PycharmProjects/PMP/IntradayMomentum/Scanner/{}_scanner.csv".format(self.date)
        levels = ["rank", "ticker","sec_type"]
        df = pd.DataFrame(columns=levels)
        df.to_csv(path, index=False)


    def append_dict_as_row(self,file_name, dict_of_elem, field_names):
        # Open file in append mode
        with open(file_name, 'a+', newline='') as write_obj:
            # Create a writer object from csv module
            dict_writer = DictWriter(write_obj, fieldnames=field_names)
            # Add dictionary as wor in the csv
            dict_writer.writerow(dict_of_elem)

    def scannerData(self, reqId: int, rank: int, contractDetails: ContractDetails,
                    distance: str, benchmark: str, projection: str, legsStr: str):
        super().scannerData(reqId, rank, contractDetails, distance, benchmark, projection, legsStr)

        if contractDetails.contract.symbol.isalnum():
            path = "/Users/alperoner/PycharmProjects/PMP/IntradayMomentum/Scanner/{}_scanner.csv".format(self.date)
            row = {"rank":rank+1, "ticker":contractDetails.contract.symbol, "sec_type":contractDetails.contract.secType}
            self.append_dict_as_row(path, row, ["rank", "ticker", "sec_type"])


    def scannerDataEnd(self, reqId: int):

        super().scannerDataEnd(reqId)
        print("ScannerDataEnd. ReqId:", reqId)
        self.cancelScannerSubscription(reqId)
        self.done = True
        self.disconnect()

    def error(self, reqId:TickerId, errorCode:int, errorString:str):
        print("Error: ", reqId, " ", errorCode," ", errorString)

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)

        print("setting nextValidOrderId: {}".format(orderId))
        self.nextValidOrderId = orderId
        print("NextValidId:", orderId)
        # ! [nextvalidid]

        # we can start now
        self.start()

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    def start(self):
        self.create_csv_files()
        self.reqScannerSubscription(self.nextOrderId(), self.subscription, [], [])



def main():

    scanSub = ScannerSubscription()
    scanSub.instrument = "STK"
    scanSub.locationCode = "STK.US.MAJOR"
    scanSub.scanCode = "TOP_PERC_GAIN"
    scanSub.marketCapAbove = 8000000000

    app = TestApp(scanSub)
    app.connect("127.0.0.1", 7497, 1)

    app.run()


    #data = app.data.reset_index(drop=True)
    #data.to_csv('/Users/alperoner/Desktop/marketscanner_dataframe.csv', index=None, header=True)


if __name__ == "__main__":

    main()


## Add a if clause to check if market is closed
##Possible market open or closed can be a function in tws api then you can use it to decrease the code to 2 lines
## 1 morning 1 afternoon

#    schedule.every().monday.at("10:14").do(main)
 #   schedule.every().tuesday.at("10:14").do(main)
  #  schedule.every().wednesday.at("10:14").do(main)
   # schedule.every().thursday.at("10:14").do(main)
    #schedule.every().friday.at("10:14").do(main)

    #while True:
     #   schedule.run_pending()
      #  time.sleep(30)