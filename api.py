import os

from numpy import empty
import database as db
import timekeeper as tk
import logsetup as ls
import time

from time import sleep

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest, GetCalendarRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderStatus

from alpaca.data import StockHistoricalDataClient, StockBarsRequest, StockLatestBarRequest, StockLatestQuoteRequest, StockLatestTradeRequest, TimeFrame
from alpaca.data.live import StockDataStream

from alpaca.broker import BrokerClient


try:
    apiKeyID = os.getenv('API_KEY_ID')
    secretKey = os.getenv('SECRET_KEY')
    paperAccount = os.getenv('PAPER_ACCOUNT') == 'True'
    liveData = {}

    data_client = StockHistoricalDataClient(apiKeyID, secretKey)
    trading_client = TradingClient(apiKeyID, secretKey, paper=paperAccount)
    broker_client = BrokerClient(apiKeyID, secretKey, sandbox=paperAccount)
    wss_client = StockDataStream(apiKeyID, secretKey)
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
        orderWaitIterations = int(os.getenv('ORDER_WAIT_ITERATIONS'))
        orderWaitSeconds = int(os.getenv('ORDER_WAIT_SECONDS'))
        for i in range(orderWaitIterations):    
            orderStatus = trading_client.get_order_by_id(order_id=orderID).status
            if orderStatus == OrderStatus.FILLED:
                return True
            elif orderStatus == OrderStatus.CANCELED:
                return False
            sleep(orderWaitSeconds)
        return False
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


def getLatestTrade(symbol):
    try:
        stockLatestTradeRequest = StockLatestTradeRequest(
                                    symbol_or_symbols=symbol
        )
        return data_client.get_stock_latest_trade(stockLatestTradeRequest)[symbol]
    except:
        ls.log.exception("api.getLatestTrade")


def getAccountInformation():
    try:
        accountInformation = trading_client.get_account()
        return accountInformation
    except:
        ls.log.exception("api.getAccountInformation")


async def liveQuoteDataHandler(data):
    try:
        liveData[data.symbol] = data
    except:
        ls.log.exception("api.liveQuoteDataHandler")


def subscribeLiveQuotes(symbol):
    try:
        wss_client.subscribe_quotes(liveQuoteDataHandler, symbol)
        waitForLiveDataSeconds = int(os.getenv('WAIT_FOR_LIVE_DATA_SECONDS'))
        startTime = time.time()
        while symbol not in liveData:
            elapsedTime = time.time() - startTime
            if (elapsedTime > waitForLiveDataSeconds):
                break
            else:
                pass
    except:
        ls.log.exception("api.subscribeLiveQuotes")


def unSubscribeLiveQuotes(symbol):
    try:
        wss_client.unsubscribe_quotes(symbol)
    except:
        ls.log.exception("api.unSubscribeLiveQuotes")


def startLiveDataStream():
    try:
        wss_client.run()
    except:
        ls.log.exception("api.startLiveDataStream")


def stopLiveDataStream():
    try:
        wss_client.stop()
    except:
        ls.log.exception("api.stopLiveDataStream")