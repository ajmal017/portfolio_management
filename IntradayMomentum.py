import datetime
import logging
import time
from csv import DictWriter
from threading import Timer
import pandas as pd
import os
from ibapi.commission_report import CommissionReport
from ibapi.common import TickerId, TickAttrib, BarData
from ibapi.client import EClient, OrderId  ##Outgoing messages
from ibapi.execution import Execution
from ibapi.order import Order
from ibapi.order_state import OrderState
from ibapi.wrapper import EWrapper ##Incoming messages
from ibapi.contract import Contract, ContractDetails
from datetime import date
import schedule

class IntradayMomentum(EWrapper,EClient):

    isMorning = False


    def __init__(self, isMorning, quantity_each, history, scanner, morning_executions, n_stocks = 5):
        EClient.__init__(self,self)

        self.orderId = 0
        self.date = datetime.date.today().strftime("%d%m%Y")
        self.history = history
        self.scanner = scanner
        self.n_stocks = n_stocks
        self.quantity_each = quantity_each

        self.cr_path = "/Users/alperoner/PycharmProjects/PMP/IntraDayMomentum/Executions/{}_commission_report.csv".format(self.date)
        self.ee_path = "/Users/alperoner/PycharmProjects/PMP/IntraDayMomentum/Executions/{}_evening_executions.csv".format(self.date)
        self.me_path = "/Users/alperoner/PycharmProjects/PMP/IntraDayMomentum/Executions/{}_morning_executions.csv".format(self.date)
        self.sh_path =  "/Users/alperoner/PycharmProjects/PMP/IntraDayMomentum/strategy_history.csv"

        self.cr_columns = ["exec_id","realized_pnl"]
        self.ee_columns = ["date","exec_id", "symbol","sec_type","quantity_sold","avg_price_sold"]
        self.me_columns = ["date", "exec_id", "symbol", "sec_type", "quantity_bought", "avg_price_bought"]
        self.sh_columns = ["exec_id","realized_pnl",
                          "date","exec_id", "symbol",
                          "sec_type","quantity_sold",
                          "avg_price_sold","quantity_bought",
                          "avg_price_bought"]

        ## Should check if these files exist
        self.commission_report = self.check_file(self.cr_path,self.cr_columns)
        self.evening_executions = self.check_file(self.ee_path,self.ee_columns)
        self.morning_executions = self.check_file(self.me_path,self.me_columns)
        self.strategy_history = self.check_file(self.sh_path, self.sh_columns)

    def check_file(self, path, columns):

        if os.path.isfile(path):
            print ("File exist at path: {}".format(path))
            data = pd.read_csv(path)
        else:
            data = pd.DataFrame(columns=[columns]).to_csv(path, index=False)

        return data

    def append_dict_as_row(self,file_name, dict_of_elem, field_names):
        # Open file in append mode
        with open(file_name, 'a+', newline='') as write_obj:
            # Create a writer object from csv module
            dict_writer = DictWriter(write_obj, fieldnames=field_names)
            # Add dictionary as wor in the csv
            dict_writer.writerow(dict_of_elem)

    ## OrderId sikikliğini çöz
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextorderId = orderId + 1
        #self.Routine(n_stocks=self.n_stocks,isMorning=self.isMorning) ##Shouldnt be here
        return self.nextorderId



    def orderStatus(self, orderId:OrderId , status:str, filled:float,
                    remaining:float, avgFillPrice:float, permId:int,
                    parentId:int, lastFillPrice:float, clientId:int,
                    whyHeld:str, mktCapPrice: float):

        print("OrderStatus. Id: ", orderId, ", Status: ", status, " Filled: ", filled, " Remaining: ", remaining,
              " Last Fill Price: ", lastFillPrice, "Avg Fill Price: ", avgFillPrice)



    def openOrder(self, orderId:OrderId, contract:Contract, order:Order,
                  orderState:OrderState):
        print("OpenOrder. Id: ", orderId, contract.symbol, contract.secType,"@", contract.exchange,":", order.action,
              order.orderType,order.totalQuantity, orderState.status)

    def execDetails(self, reqId:int, contract:Contract, execution:Execution):
        print("ExecDetails. ", reqId, contract.symbol, contract.secType, contract.currency, execution.execId,
              execution.orderId, execution.shares,execution.avgPrice)


        if self.isMorning:
            row = {"date": [self.today], "exec_id":[execution.execId],
                   "symbol": [contract.symbol],"sec_type": [contract.secType],
                   "quantity_bought": [execution.cumQty], "avg_price_bought": [execution.avgPrice]}

            self.append_dict_as_row(self.me_path,row,self.me_columns)

        else:
            row = {"date": [self.today], "exec_id":[execution.execId],
                   "symbol": [contract.symbol], "sec_type": [contract.secType],
                   "quantity_sold": [execution.cumQty], "avg_price_sold": [execution.avgPrice]}

            self.append_dict_as_row(self.ee_path, row, self.ee_columns)

    def commissionReport(self, commissionReport:CommissionReport):
        super().commissionReport(commissionReport)
        print("CommissionReport.", commissionReport)

        if not self.isMorning:
            row = {"exec_id": [commissionReport.execId], "realized_pnl": [commissionReport.realizedPNL]}
            self.append_dict_as_row(self.cr_path, row, self.cr_columns)

    def error(self, reqId:TickerId, errorCode:int, errorString:str):
        print("Error: ", reqId, " ", errorCode," ", errorString)

    def USStock(self,ticker: str):
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        return contract

    def getOrder(self, quantity_each:int, action: str):
        order = Order()
        order.action = action
        order.totalQuantity = quantity_each
        order.orderType = "MKT"
        return order



    def end_day(self):

        me = pd.read_csv(self.me_path)
        ee = pd.read_csv(self.ee_path)
        cr = pd.read_csv(self.cr_path)

        data = me.merge(ee, how= "inner")
        data = data.merge(cr, how="inner")

        data["return"] = 100 * data.realized_pnl / (data.AvgPrice_Bought*data.quantity_bought)

        data.to_csv(self.sh_path, mode='a', header=False)

        self.done = True
        self.disconnect()

    ##Burda kaldın

    def Routine(self, n_stocks):

        if self.isMorning:
            stocks = self.scanner.loc[0:n_stocks-1]
            symbols = stocks.Symbol.values
            order = self.getOrder(quantity_each=self.quantity_each, action="BUY")

        else:
            stocks = self.morning_executions
            symbols = stocks.Symbol.values
            order = self.getOrder(quantity_each=self.quantity_each, action="SELL")


        for symbol in symbols:
            contract = self.USStock(symbol)
            time.sleep(2)
            self.placeOrder(self.nextOrderId(),contract,order)
            time.sleep(2)

        print("Routine completed")

