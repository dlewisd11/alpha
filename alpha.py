from dotenv import load_dotenv
load_dotenv()

import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import logsetup as ls
import timekeeper as tk
import database as db
import api

from asset import Asset
from time import sleep


def main():

    try:
        ls.log.info("BEGIN")

        weekDayCondition = tk.weekDay != 5 and tk.weekDay != 6
        marketClock = api.getMarketClock()
        marketOpenCondition = marketClock.is_open
        marketCloseCondition = tk.hour == (marketClock.next_close.hour - 1)
        runUnconditionally = os.getenv('RUN_UNCONDITIONALLY') == 'True'

        if (weekDayCondition and marketOpenCondition and marketCloseCondition) or runUnconditionally:

            buyEnabled = os.getenv('BUY_ENABLED') == 'True'
            sellEnabled = os.getenv('SELL_ENABLED') == 'True'
            ordersEnabled = os.getenv('ORDERS_ENABLED') == 'True'

            logData = {
                        'buyEnabled': buyEnabled,
                        'sellEnabled': sellEnabled,
                        'ordersEnabled': ordersEnabled
            }

            ls.log.info(logData)
            
            symbolList = os.getenv('TICKERS').split(',')

            pool = ThreadPoolExecutor(1)
            pool.submit(api.startLiveDataStream)
            sleep(5)

            try:

                for symbol in symbolList:
                    api.subscribeLiveData(symbol)

                sleep(int(os.getenv('WAIT_FOR_LIVE_DATA_SECONDS')))

                if buyEnabled:

                    for symbol in symbolList:
                        rsiPeriod = int(os.getenv('RSI_PERIOD_BUY'))
                        asset = Asset(symbol, rsiPeriod)
                        rsi = asset.rsi
                        percentUpDown = asset.percentUpDown
                        limitPriceBuy = asset.limitPriceBuy
                        rsiLower = int(os.getenv('RSI_LOWER'))

                        #buy
                        if percentUpDown <= 0 and rsi <= rsiLower:

                            quantity = getBuyOrderQuantity(symbol, limitPriceBuy)
                                
                            if quantity > 0 and ordersEnabled:
                                orderID = api.submitOrder(symbol, quantity, limitPriceBuy, 'buy')
                                orderFilled = api.orderFilled(orderID)
                                
                                if orderFilled:
                                    purchasePrice = api.getFilledOrderAveragePrice(orderID)
                                    insertBuyRecord(symbol, quantity, purchasePrice, orderID)

                        api.unSubscribeLiveData(symbol)

                #sell
                if sellEnabled:

                    distinctSymbolsEligibleForSale = getDistinctSymbolsEligibleForSale()
                    for record in distinctSymbolsEligibleForSale:
                        symbol = record[0]
                        api.subscribeLiveData(symbol)

                    sleep(int(os.getenv('WAIT_FOR_LIVE_DATA_SECONDS')))

                    accountInformation = api.getAccountInformation()
                    cash = float(accountInformation.cash)
                    positions = getOpenPositionsEligibleForSale()
                    
                    for position in positions:

                        tableRecordID = position[0]
                        symbol = position[1]
                        quantity = float(str(position[2]))
                        purchaseDate = position[3]
                        purchasePrice = float(position[4])

                        elapsedTimeSellCondition = purchaseDate < tk.formattedDate

                        if elapsedTimeSellCondition:

                            rsiPeriod = int(os.getenv('RSI_PERIOD_SELL'))
                            asset = Asset(symbol, rsiPeriod)
                            limitPriceSell = asset.limitPriceSell
                            percentUpDown = asset.percentUpDown
                            rsi = asset.rsi

                            convertedPurchaseDate = tk.stringToDate(purchaseDate)
                            sellSideMarginMinimum = float(os.getenv('SELL_SIDE_MARGIN_MINIMUM'))
                            marginInterestRate = float(os.getenv('MARGIN_INTEREST_RATE'))
                            marginInterestCoverage = 0 if cash >= 0 else tk.dateDiff(convertedPurchaseDate, tk.currentDate) * (marginInterestRate / 360)
                            rsiUpper = int(os.getenv('RSI_UPPER'))

                            percentUpDownCondition = percentUpDown > 0
                            rsiSellCondition = rsi >= rsiUpper
                            profitMarginSellCondition = ((limitPriceSell / purchasePrice) - 1) >= (sellSideMarginMinimum + marginInterestCoverage)

                            if percentUpDownCondition and profitMarginSellCondition and rsiSellCondition and ordersEnabled:

                                orderID = api.submitOrder(symbol, quantity, limitPriceSell, 'sell')
                                orderFilled = api.orderFilled(orderID)
                                
                                if orderFilled:
                                    salePrice = api.getFilledOrderAveragePrice(orderID)
                                    updateSoldPosition(tableRecordID, salePrice, orderID)

                    for record in distinctSymbolsEligibleForSale:
                        symbol = record[0]
                        api.unSubscribeLiveData(symbol)

            except:
                ls.log.exception("alpha.main inner")
            
            finally:
                api.stopLiveDataStream()
                pool.shutdown()

        else:
            ls.log.info("Run conditions not met. Today is a weekend day, the market is not open, or the market is not closing in the next hour.")

    except:
        ls.log.exception("alpha.main")
    
    finally:
        ls.log.info("END")

        
def getBuyOrderQuantity(symbol, limitPrice):
    try:
        enduranceDays = int(os.getenv('ENDURANCE_DAYS'))

        allowMarginTrading = os.getenv('ALLOW_MARGIN_TRADING') == 'True'
        accountInformation = api.getAccountInformation()
        equity = float(accountInformation.equity)
        cash = float(accountInformation.cash)
        longMarketValue = float(accountInformation.long_market_value)

        if allowMarginTrading:
            activeMarginPercentage = float(os.getenv('ACTIVE_MARGIN_PERCENTAGE'))
            activeCapital = equity * (1 + activeMarginPercentage)
            theoreticalOrderCost = activeCapital / enduranceDays
            activeBuyingPower = activeCapital - longMarketValue
        
        else:
            activeBuyingPower = cash

        if (activeBuyingPower / theoreticalOrderCost) > 0:
            orderQuantity = int(theoreticalOrderCost / limitPrice)
            return orderQuantity
        
        else:
            orderQuantity = int(activeBuyingPower / limitPrice)
            return orderQuantity if orderQuantity > 0 else 0

    except:
        ls.log.exception("alpha.getBuyOrderQuantity")


def insertBuyRecord(symbol, quantity, purchasePrice, orderID):
    try:
        tableRecordID = str(uuid.uuid4())
        query = "INSERT INTO " + db.dbTableName + " (id, symbol, quantity, purchasedate, purchaseprice, purchaseorderid) VALUES (%s, %s, %s, %s, %s, %s)"
        values = (tableRecordID, symbol, quantity, tk.formattedDate, purchasePrice, orderID)
        db.runQuery(query, values)
    except:
        ls.log.exception("alpha.insertBuyRecord")


def updateSoldPosition(tableRecordID, salePrice, sellOrderID):
    try:
        query = "UPDATE " + db.dbTableName + " SET saledate = %s, saleprice = %s, saleorderid = %s WHERE id = %s"
        values = (tk.formattedDate, salePrice, sellOrderID, tableRecordID)
        db.runQuery(query, values)
    except:
        ls.log.exception("alpha.updateSoldPosition")


def getOpenPositionsEligibleForSale():
    try:
        query = "SELECT id, symbol, quantity, purchasedate, purchaseprice FROM " + db.dbTableName + " WHERE ISNULL(saleorderid) AND ISNULL(saledate) AND ISNULL(saleprice)"
        positions = db.runQueryAndReturnResults(query, ())
        return positions
    except:
        ls.log.exception("alpha.getOpenPositionsEligibleForSale")


def getDistinctSymbolsEligibleForSale():
    try:
        query = "SELECT DISTINCT symbol FROM " + db.dbTableName + " WHERE ISNULL(saleorderid) AND ISNULL(saledate) AND ISNULL(saleprice)"
        symbols = db.runQueryAndReturnResults(query, ())
        return symbols
    except:
        ls.log.exception("alpha.getDistinctSymbolsEligibleForSale")


if __name__ == '__main__':
    try:
        main()
    except:
        ls.log.exception("alpha")
    finally:
        quit()

