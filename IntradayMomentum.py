import datetime
import logging
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
from ibapi.wrapper import EWrapper, TickType  ##Incoming messages
from ibapi.contract import Contract, ContractDetails




class IntradayMomentum(EWrapper,EClient):

    def __init__(self,
                 isMorning,
                 invest_on_each=None,
                 first_n_stock = 5):

        EClient.__init__(self,self)

        self.nextValidOrderId = None
        self.isMorning = isMorning
        self.date = datetime.date.today().strftime("%d%m%Y")
        self.started = False
        self.globalCancelOnly = False
        self.first_n_stock = first_n_stock

        if isMorning and invest_on_each is None:
            raise("You need to give investment amount")

        self.invest_on_each = invest_on_each

        self.scanner_path = "/Users/alperoner/PycharmProjects/PMP/IntradayMomentum/Scanner/{}_scanner.csv".format(self.date)

        self.dp_path = "/Users/alperoner/PycharmProjects/PMP/IntraDayMomentum/DailyPrices"
        self.cr_path = "/Users/alperoner/PycharmProjects/PMP/IntraDayMomentum/Executions/{}_commission_report.csv".format(self.date)
        self.ee_path = "/Users/alperoner/PycharmProjects/PMP/IntraDayMomentum/Executions/{}_evening_executions.csv".format(self.date)
        self.me_path = "/Users/alperoner/PycharmProjects/PMP/IntraDayMomentum/Executions/{}_morning_executions.csv".format(self.date)
        self.sh_path =  "/Users/alperoner/PycharmProjects/PMP/IntraDayMomentum/strategy_history.csv"

        self.cr_columns = ["exec_id","realized_pnl"]
        self.ee_columns = ["date","exec_id", "ticker", "sec_type","quantity_sold","avg_price_sold"]
        self.me_columns = ["date", "exec_id", "ticker", "sec_type", "quantity_bought", "avg_price_bought"]

        if os.path.isfile(self.scanner_path):
            self.scanner = pd.read_csv(self.scanner_path)
        else:
             raise("Scanner file does not exist")

        ## Should check if these files exist
        self.commission_report = self.check_file(self.cr_path,self.cr_columns)
        self.evening_executions = self.check_file(self.ee_path,self.ee_columns)
        self.morning_executions = self.check_file(self.me_path,self.me_columns)


    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)

        print("setting nextValidOrderId: {}".format(orderId))
        self.nextValidOrderId = orderId
        print("NextValidId:", orderId)
    # ! [nextvalidid]

        # we can start now
        self.start()

    def orderStatus(self, orderId:OrderId , status:str, filled:float,
                    remaining:float, avgFillPrice:float, permId:int,
                    parentId:int, lastFillPrice:float, clientId:int,
                    whyHeld:str, mktCapPrice: float):
        print("")
#       print("OrderStatus. Id: ", orderId, ", Status: ", status,
#              " Filled: ", filled, " Remaining: ", remaining,
#              " Last Fill Price: ", lastFillPrice, "Avg Fill Price: ", avgFillPrice)

    def openOrder(self, orderId:OrderId, contract:Contract, order:Order,
                  orderState:OrderState):
        print("OpenOrder. Id: ", orderId, contract.symbol, contract.secType,"@", contract.exchange,":", order.action,
              order.orderType,order.totalQuantity, orderState.status)

    def execDetails(self, reqId:int, contract:Contract, execution:Execution):
        print("ExecDetails. ", reqId, contract.symbol, contract.secType, contract.currency, execution.execId,
              execution.orderId, execution.shares,execution.avgPrice)

        if self.isMorning:
            row = {"date": self.date, "exec_id": execution.execId,
                   "ticker": contract.symbol, "sec_type": contract.secType,
                   "quantity_bought": execution.shares,
                   "avg_price_bought": execution.avgPrice}

            self.append_dict_as_row(self.me_path,row,self.me_columns)

        else:
            row = {"date": self.date, "exec_id":execution.execId,
                   "ticker": contract.symbol, "sec_type": contract.secType,
                   "quantity_sold": execution.shares, "avg_price_sold": execution.avgPrice}

            self.append_dict_as_row(self.ee_path, row, self.ee_columns)

    def tickPrice(self, reqId:TickerId , tickType:TickType, price:float,
                  attrib:TickAttrib):
        print("TickerId: ", reqId, "Price: ", price)

    def tickSnapshotEnd(self, reqId:int):
        print("Snapshot ended")

    def execDetailsEnd(self, reqId:int):
        print("Execution ended")

    def commissionReport(self, commissionReport:CommissionReport):
        super().commissionReport(commissionReport)
        print("CommissionReport.", commissionReport)

        if not self.isMorning:
            row = {"exec_id": commissionReport.execId, "realized_pnl": commissionReport.realizedPNL}
            self.append_dict_as_row(self.cr_path, row, self.cr_columns)

    def error(self, reqId:TickerId, errorCode:int, errorString:str):
        print("Error: ", reqId, " ", errorCode," ", errorString)

    def check_file(self, path, columns):

        if os.path.isfile(path):
            print ("File exist at path: {}".format(path))
            data = pd.read_csv(path)
        else:
            data = pd.DataFrame(columns=columns).to_csv(path, index=False)

        return data

    def append_dict_as_row(self,file_name, dict_of_elem, field_names):
        # Open file in append mode
        with open(file_name, 'a+', newline='') as write_obj:
            # Create a writer object from csv module
            dict_writer = DictWriter(write_obj, fieldnames=field_names)
            # Add dictionary as wor in the csv
            dict_writer.writerow(dict_of_elem)

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    def start(self):

        if self.started:
            return
        self.started = True
        ##Questionable
        if self.globalCancelOnly:
            print("Executing GlobalCancel only")
            self.reqGlobalCancel()
        else:
            self.routine()


    def execute_orders(self,tickers, isMorning):

        if isMorning:
            action = "BUY"
            price_dict = self.get_price_dict(tickers= tickers)
            quantity_dict = self.calculate_quantities(price_dict= price_dict)

            for ticker in tickers:
                contract = self.get_contract(ticker)
                order = self.get_order(quantity=quantity_dict[ticker], action= action)
                self.placeOrder(self.nextOrderId(), contract, order)
        else:

            action = "SELL"
            quantity_dict = self.get_morning_quantities()

            for ticker in tickers:
                contract = self.get_contract(ticker)
                order = self.get_order(quantity=quantity_dict[ticker], action=action)
                self.placeOrder(self.nextOrderId(), contract, order)


    def get_morning_quantities(self):

        morning_data = pd.read_csv(self.me_path)
        morning_quantities = dict(zip(morning_data.ticker, morning_data.quantity_bought))
        return morning_quantities


    def get_contract(self,ticker: str):
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        return contract

    def contract_list(self, tickers):
        return [self.get_contract(ticker) for ticker in tickers]

    def get_order(self, quantity:int, action: str):
        order = Order()
        order.action = action
        order.totalQuantity = quantity
        order.orderType = "MTL" ##Play with ordertype to maximize profits
        return order



    def end_day(self):

        me = pd.read_csv(self.me_path)
        ee = pd.read_csv(self.ee_path)
        cr = pd.read_csv(self.cr_path)


        if len(me) == 0 or me is None:
            raise("Morning Executions did not establish")
        if len(ee) == 0 or ee is None:
            raise("Evening Executions did not establish")

        df_merged = pd.merge(ee, cr, on= ["exec_id"], how = "inner")
        df_merged = pd.merge(me, df_merged, on = ["ticker", "date", "sec_type"], how = "inner")


        if os.path.isfile(self.sh_path):
            df_merged.to_csv(self.sh_path, mode='a', header=False, index = False)
        else:
            df_merged.to_csv(self.sh_path, index= False)


    def remove_files(self, paths):
        for path in paths:
            os.remove(path)


    ##Call stop after 3 seconds to disconnect program
    def stop(self):
        #self.remove_files([self.me_path, self.ee_path, self.cr_path])
        self.done = True
        self.disconnect()


    def check_price(self, ticker):

        file_name = "{}_{}_history.csv".format(ticker, self.date)
        path = os.path.join(self.dp_path, file_name)
        data = pd.read_csv(path)

        if data is None or len(data) == 0:
            raise("Daily price data does not exist")
        elif len(data) == 1:
            price = data.close.values[0]
        else:
            raise("More than one price to pick")

        return price


    def get_price_dict(self, tickers):

        price_dict = {}

        for ticker in tickers:
            price_dict[ticker] = self.check_price(ticker)

        return price_dict

        ##Use daily prices in the folder to receive price and stock quantity data

    def calculate_quantities(self, price_dict) -> {}:

        quantity_dict = {}

        for ticker, price in price_dict.items():
            quantity_dict[ticker] = int(self.invest_on_each / price)

        print(quantity_dict)
        return quantity_dict



    def routine(self):

        if self.isMorning:
            end_index = self.first_n_stock-1
            stocks = self.scanner.loc[0:end_index]
            tickers = stocks.ticker.values

        else:
            stocks = pd.read_csv(self.me_path)
            tickers = stocks.ticker.values


        self.execute_orders(tickers, self.isMorning)
        print("Routine completed")


def morning():
    strategy = IntradayMomentum(isMorning= True, invest_on_each=1000, first_n_stock=10)
    strategy.connect("127.0.0.1", 7497, 1)
    Timer(20, strategy.stop).start()
    strategy.run()

def evening():
    strategy = IntradayMomentum(isMorning= False)
    strategy.connect("127.0.0.1", 7497, 1)
    Timer(20, strategy.end_day).start()
    Timer(25, strategy.stop).start()
    strategy.run()


morning()



evening()