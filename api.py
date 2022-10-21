import os
import database as db
import timekeeper as tk
import logsetup as ls

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest, GetCalendarRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderStatus

from alpaca.data import StockHistoricalDataClient, StockBarsRequest, StockLatestBarRequest, StockLatestQuoteRequest, TimeFrame

from alpaca.broker import BrokerClient

from time import sleep


try:
    apiKeyID = os.getenv('API_KEY_ID')
    secretKey = os.getenv('SECRET_KEY')
    paperAccount = os.getenv('PAPER_ACCOUNT') == 'True'

    data_client = StockHistoricalDataClient(apiKeyID, secretKey)
    trading_client = TradingClient(apiKeyID, secretKey, paper=paperAccount)
    broker_client = BrokerClient(apiKeyID, secretKey, sandbox=paperAccount)
except:
    ls.log.error("Error initializing api, quitting program.")
    ls.log.exception("api")
    quit()


def isMarketOpen():
    try:
        return trading_client.get_clock().is_open
    except:
        ls.log.exception("api.isMarketOpen")


def submitOrder(symbol, quantity, limitPrice, orderSide):

    try:
        limit_order_data = LimitOrderRequest(
                            side=OrderSide.BUY if orderSide == 'buy' else OrderSide.SELL,
                            symbol=symbol,
                            qty=quantity,
                            limit_price=limitPrice,
                            time_in_force=TimeInForce.FOK
        )

        orderID = str(trading_client.submit_order(order_data=limit_order_data).id)

        ls.log.info(str(orderSide + " order id " + orderID + " submitted"))

        return orderID

    except:
        ls.log.exception("api.submitOrder")


def orderFilled(orderID):
    try:
        orderFilled = False
        for i in range(int(os.getenv('ORDER_WAIT_ITERATIONS'))):
            orderFilled = trading_client.get_order_by_id(order_id=orderID).status == OrderStatus.FILLED
            if orderFilled:
                break
            sleep(int(os.getenv('ORDER_WAIT_SECONDS')))
        return orderFilled
    except:
        ls.log.exception("api.orderFilled")


def getFilledOrderAveragePrice(orderID):
    try:
        filledAveragePrice = trading_client.get_order_by_id(order_id=orderID).filled_avg_price
        return filledAveragePrice
    except:
        ls.log.exception("api.getFilledOrderAveragePrice")


def getTradingCalendar():
    try:
        calendarStartDate = tk.todayMinus30Days
        calendarRequest = GetCalendarRequest(start=calendarStartDate, end=tk.today)
        tradingCalendar = trading_client.get_calendar(calendarRequest)
        return tradingCalendar
    except:
        ls.log.exception("api.getTradingCalendar")


def getStockBars(symbol, startDate, endDate):
    try:
        stockBarsRequest = StockBarsRequest(
                                symbol_or_symbols=symbol,
                                start=startDate,
                                end=endDate,
                                timeframe=TimeFrame.Day
                                    
        )
        return data_client.get_stock_bars(stockBarsRequest)
    except:
        ls.log.exception("api.getStockBars")


def getStockLatestBar(symbol):
    try:
        stockLatestBarRequest = StockLatestBarRequest(
                                symbol_or_symbols=symbol
                                    
        )
        return data_client.get_stock_latest_bar(stockLatestBarRequest)
    except:
        ls.log.exception("api.getStockLatestBar")


def getLatestQuote(symbol):
    try:
        stockLatestQuoteRequest = StockLatestQuoteRequest(
                                    symbol_or_symbols=symbol
        )
        return data_client.get_stock_latest_quote(stockLatestQuoteRequest)[symbol]
    except:
        ls.log.exception("api.getLatestQuote")


def getAccountInformation():
    try:
        accountInformation = trading_client.get_account()
        return accountInformation
    except:
        ls.log.exception("api.getAccountInformation")