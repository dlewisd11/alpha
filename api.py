import os

import timekeeper as tk
import logsetup as ls
import requests

from time import sleep

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest, GetCalendarRequest, GetPortfolioHistoryRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderStatus

from alpaca.data import StockHistoricalDataClient, StockBarsRequest, StockLatestBarRequest, StockLatestQuoteRequest, StockLatestTradeRequest, TimeFrame
from alpaca.data.live import StockDataStream

from alpaca.broker.client import BrokerClient
from alpaca.broker.requests import GetAccountActivitiesRequest


try:
    apiKeyID = os.getenv('API_KEY_ID')
    secretKey = os.getenv('SECRET_KEY')
    secondaryDataSourceApiUrl = os.getenv('SECONDARY_DATA_SOURCE_API_URL')
    secondaryDataSourceApiKey = os.getenv('SECONDARY_DATA_SOURCE_API_KEY')
    paperAccount = os.getenv('PAPER_ACCOUNT') == 'True'
    
    specialHeaders = {'APCA-API-KEY-ID': apiKeyID, 'APCA-API-SECRET-KEY': secretKey}

    liveQuoteData = {}
    liveTradeData = {}

    data_client = StockHistoricalDataClient(apiKeyID, secretKey)
    trading_client = TradingClient(apiKeyID, secretKey, paper=paperAccount)
    broker_client = BrokerClient(apiKeyID, secretKey, sandbox=paperAccount)
    wss_client = StockDataStream(apiKeyID, secretKey)
except:
    ls.log.error("Error initializing api, quitting program.")
    ls.log.exception("api")
    quit()


def getMarketClock():
    try:
        return trading_client.get_clock()
    except:
        ls.log.exception("api.getMarketClock")
        quit()


def decodeTimeInForce(timeInForce):
    try:
        match timeInForce:
            case 'fok':
                timeInForce = TimeInForce.FOK
            case 'day':
                timeInForce = TimeInForce.DAY
            case 'cls':
                timeInForce = TimeInForce.CLS
            case 'gtc':
                timeInForce = TimeInForce.GTC
            case 'ioc':
                timeInForce = TimeInForce.IOC
            case 'opg':
                timeInForce = TimeInForce.OPG
            case _:
                timeInForce = TimeInForce.FOK
        return timeInForce
    except:
        ls.log.exception("api.decodeTimeInForce")


def submitOrder(symbol, quantity, limitPrice, orderSide):

    try:
        timeInForce = decodeTimeInForce(os.getenv('TIME_IN_FORCE'))

        limit_order_data = LimitOrderRequest(
                            side=OrderSide.BUY if orderSide == 'buy' else OrderSide.SELL,
                            symbol=symbol,
                            qty=quantity,
                            limit_price=limitPrice,
                            time_in_force=timeInForce
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
                ls.log.info(str("order id " + orderID + " filled"))
                return True
            elif orderStatus == OrderStatus.CANCELED:
                ls.log.info(str("order id " + orderID + " canceled"))
                return False
            sleep(orderWaitSeconds)

        trading_client.cancel_order_by_id(order_id=orderID)
        ls.log.info(str("order id " + orderID + " not filled, canceled"))
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
        calendarRequest = GetCalendarRequest(start=calendarStartDate, end=tk.currentDateTime)
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
        liveQuoteData[data.symbol] = data
    except:
        ls.log.exception("api.liveQuoteDataHandler")


async def liveTradeDataHandler(data):
    try:
        liveTradeData[data.symbol] = data
    except:
        ls.log.exception("api.liveTradeDataHandler")


def subscribeLiveData(symbol):
    try:
        wss_client.subscribe_quotes(liveQuoteDataHandler, symbol)
        wss_client.subscribe_trades(liveTradeDataHandler, symbol)
    except:
        ls.log.exception("api.subscribeLiveQuotes")


def unSubscribeLiveData(symbol):
    try:
        wss_client.unsubscribe_quotes(symbol)
        wss_client.unsubscribe_trades(symbol)
        if symbol in liveQuoteData: del liveQuoteData[symbol]
        if symbol in liveTradeData: del liveTradeData[symbol]
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


def getSecondaryPrice(symbol):
    try:
        url = secondaryDataSourceApiUrl.replace('^{SYMBOL}', symbol).replace('^{KEY}', secondaryDataSourceApiKey)
        response = requests.get(url)
        if not response.ok:
            raise Exception("Error contacting secondary data source api.")
        jsonResponse = response.json()
        price = jsonResponse[0]['price']
        return price
    except:
        ls.log.exception("api.getSecondaryPricing")


#######################################################################
## All methods below use custom requests due to gaps in native APIs. ##
#######################################################################

def getCashDeposits(afterTimestamp):
    try:
        #This method should eventually use broker_client.get_account_activities()
        url = 'https://api.alpaca.markets/v2/account/activities/CSD?after=' + afterTimestamp
        response = requests.get(url=url, headers=specialHeaders).json()
        return response
    except:
        ls.log.exception("api.getCashDeposits")

def getCashWithdrawals(afterTimestamp):
    try:
        #This method should eventually use broker_client.get_account_activities()
        url = 'https://api.alpaca.markets/v2/account/activities/CSW?after=' + afterTimestamp
        response = requests.get(url=url, headers=specialHeaders).json()
        return response
    except:
        ls.log.exception("api.getCashWithdrawals")

def getPortfolioHistory():
    try:
        #This method should eventually use broker_client.get_portfolio_history_for_account()
        url = 'https://api.alpaca.markets/v2/account/portfolio/history?period=1A'
        response = requests.get(url=url, headers=specialHeaders).json()
        return response
    except:
        ls.log.exception("api.getPortfolioHistory")

