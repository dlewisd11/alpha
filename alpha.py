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
        currentHour = tk.hour
        marketOpeningTimeCondition = atTheOpen = currentHour == 9
        marketClosingTimeCondition = currentHour == (marketClock.next_close.hour - 1)
        runUnconditionally = os.getenv('RUN_UNCONDITIONALLY') == 'True'
        paperAccount = os.getenv('PAPER_ACCOUNT') == 'True'

        if (weekDayCondition and marketOpenCondition and (marketOpeningTimeCondition or marketClosingTimeCondition)) or runUnconditionally:

            buyEnabled = os.getenv('BUY_ENABLED') == 'True'
            sellEnabled = os.getenv('SELL_ENABLED') == 'True'
            ordersEnabled = os.getenv('ORDERS_ENABLED') == 'True'

            logData = {
                        'buyEnabled': buyEnabled,
                        'sellEnabled': sellEnabled,
                        'ordersEnabled': ordersEnabled,
                        'atTheOpen': atTheOpen
            }

            ls.log.info(logData)
            
            symbolList = os.getenv('TICKERS').split(',')

            pool = ThreadPoolExecutor(1)
            pool.submit(api.startLiveDataStream)
            sleep(5)

            try:

                #sell
                if sellEnabled:

                    ls.log.info("SELL PHASE")

                    distinctSymbolsEligibleForSale = getDistinctSymbolsEligibleForSale()
                    for record in distinctSymbolsEligibleForSale:
                        symbol = record[0]
                        api.subscribeLiveData(symbol)

                    sleep(int(os.getenv('WAIT_FOR_LIVE_DATA_SECONDS')))

                    accountInformation = api.getAccountInformation()
                    cash = float(accountInformation.cash)
                    positions = getOpenPositionsEligibleForSale()
                    rsiPeriodDictionary = getRsiPeriods()
                    
                    for position in positions:

                        tableRecordID = position[0]
                        symbol = position[1]
                        quantity = float(str(position[2]))
                        purchaseDate = position[3]
                        purchasePrice = float(position[4])
                        dayTradeLimitCheck = dayTradeCheck()

                        elapsedTimeSellCondition = purchaseDate < tk.formattedDate or dayTradeLimitCheck

                        if elapsedTimeSellCondition:

                            rsiPeriod = rsiPeriodDictionary['sell']
                            asset = Asset(symbol, rsiPeriod, atTheOpen)
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

                            standardSellScenario = percentUpDownCondition and profitMarginSellCondition and rsiSellCondition
                            dayTradeSellScenario = purchaseDate == tk.formattedDate and profitMarginSellCondition
                            negativeCashSellScenario = cash <= 0 and profitMarginSellCondition

                            if standardSellScenario or dayTradeSellScenario or negativeCashSellScenario:
                                
                                ls.log.debug("Sell conditions met.")

                                if ordersEnabled:
                                    orderID = api.submitOrder(symbol, quantity, limitPriceSell, 'sell')
                                    orderFilled = api.orderFilled(orderID)
                                    
                                    if orderFilled:
                                        salePrice = api.getFilledOrderAveragePrice(orderID)
                                        updateSoldPosition(tableRecordID, salePrice, orderID)

                    for record in distinctSymbolsEligibleForSale:
                        symbol = record[0]
                        api.unSubscribeLiveData(symbol)

                #buy
                if buyEnabled:

                    ls.log.info("BUY PHASE")

                    for symbol in symbolList:
                        api.subscribeLiveData(symbol)

                    sleep(int(os.getenv('WAIT_FOR_LIVE_DATA_SECONDS')))

                    rsiPeriodDictionary = getRsiPeriods()

                    for symbol in symbolList:                        
                        rsiPeriod = rsiPeriodDictionary['buy']
                        asset = Asset(symbol, rsiPeriod, atTheOpen)
                        rsi = asset.rsi
                        percentUpDown = asset.percentUpDown
                        limitPriceBuy = asset.limitPriceBuy
                        rsiLower = int(os.getenv('RSI_LOWER'))

                        if percentUpDown <= 0 and rsi <= rsiLower:
                            
                            ls.log.debug("Buy conditions met.")
                            quantity = getBuyOrderQuantity(limitPriceBuy)
                                
                            if quantity > 0 and ordersEnabled:
                                orderID = api.submitOrder(symbol, quantity, limitPriceBuy, 'buy')
                                orderFilled = api.orderFilled(orderID)
                                
                                if orderFilled:
                                    purchasePrice = api.getFilledOrderAveragePrice(orderID)
                                    insertBuyRecord(symbol, quantity, purchasePrice, orderID)

                        api.unSubscribeLiveData(symbol)

                #performance reporting
                if not paperAccount:
                    oneYearReturn = getOneYearReturn()
                    oneYearBenchmarkReturn = getOneYearBenchmarkReturn()
                    oneYearVariance = float('%.6f' % (oneYearReturn - oneYearBenchmarkReturn))

                    ls.log.info(
                                    {
                                        'oneYearPerformance': oneYearReturn,
                                        'oneYearBenchmark': oneYearBenchmarkReturn,
                                        'oneYearVariance': oneYearVariance
                                    }
                                )

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

        
def getBuyOrderQuantity(limitPrice):
    try:
        enduranceDays = int(os.getenv('ENDURANCE_DAYS'))

        accountInformation = api.getAccountInformation()
        equity = float(accountInformation.equity)
        longMarketValue = float(accountInformation.long_market_value)
        cash = float(accountInformation.cash)

        activeMarginPercentage = float(os.getenv('ACTIVE_MARGIN_PERCENTAGE'))
        activeCapital = equity * (1 + activeMarginPercentage)
        theoreticalOrderCost = activeCapital / enduranceDays
        activeBuyingPower = activeCapital - longMarketValue

        if (activeBuyingPower / theoreticalOrderCost) > 0:
            orderQuantity = int(theoreticalOrderCost / limitPrice)
            
        else:
            orderQuantity = int(activeBuyingPower / limitPrice)
            orderQuantity = orderQuantity if orderQuantity > 0 else 0

        if activeMarginPercentage <= 0:
            estimatedOrderCost = orderQuantity * limitPrice
            orderQuantity = orderQuantity if cash >= estimatedOrderCost else 0

        ls.log.debug(
                        {
                            'equity': equity,
                            'longMarketValue': longMarketValue,
                            'activeMarginPercentage': activeMarginPercentage,
                            'activeCapital': activeCapital,
                            'theoreticalOrderCost': theoreticalOrderCost,
                            'activeBuyingPower': activeBuyingPower,
                            'limitPrice': limitPrice,
                            'orderQuantity': orderQuantity        
                        }
                    )

        return orderQuantity
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


def dayTradeCheck():
    try:
        query = "SELECT COUNT(*) FROM " + db.dbTableName + " WHERE purchasedate >= %s AND purchasedate = saledate"
        count = db.runQueryAndReturnResults(query, (tk.todayMinus5DaysFormatted,))
        result = count[0][0] <= 3
        return result
    except:
        ls.log.exception("alpha.dayTradeCheck")


def getRsiPeriods():
    try:
        accountInformation = api.getAccountInformation()
        equity = float(accountInformation.equity)
        longMarketValue = float(accountInformation.long_market_value)
        activeMarginPercentage = float(os.getenv('ACTIVE_MARGIN_PERCENTAGE'))
        activeCapital = equity * (1 + activeMarginPercentage)
        capitalUtilization = longMarketValue / activeCapital

        rsiPeriodLower = int(os.getenv('RSI_PERIOD_LOWER'))
        rsiPeriodUpper = int(os.getenv('RSI_PERIOD_UPPER'))

        if rsiPeriodLower > rsiPeriodUpper:
            raise Exception("RSI period lower must be less than or equal to RSI period upper.")

        rsiRange = range(rsiPeriodLower, rsiPeriodUpper + 1)
        rsiRangeLength = len(rsiRange)

        utzSegmentSize = 1 / rsiRangeLength
        currentUtzTestValue = utzSegmentSize
        
        buyRSI = rsiPeriodUpper
        sellRSI = rsiPeriodLower

        for i in range(rsiRangeLength):
            if capitalUtilization <= currentUtzTestValue:
                
                buyRSI = rsiRange[i]                
                sellRSI = rsiRange[rsiRangeLength - 1 - i]
                
                break

            currentUtzTestValue = currentUtzTestValue + utzSegmentSize
        
        rsiDictionary = {'buy': buyRSI, 'sell': sellRSI}
        
        return rsiDictionary
    except:
        ls.log.exception("alpha.getRsiPeriods")


def getOneYearReturn():
    try:
        periodStart = str(tk.todayMinus1YearPlus2DaysFormatted)
        cashDeposits = api.getCashDeposits(periodStart)
        cashWithdrawals = api.getCashWithdrawals(periodStart)
        portfolioHistory = api.getPortfolioHistory()
        
        totalDeposits = float(0)
        for deposit in cashDeposits:
            totalDeposits += float(deposit['net_amount'])
        
        totalWithdrawals = float(0)
        for withdrawal in cashWithdrawals:
            totalWithdrawals += float(withdrawal['net_amount'])

        startingEquity = float(portfolioHistory['equity'][0])
        endingEquity = float(portfolioHistory['equity'][len(portfolioHistory['equity'])-1])

        # withdrawals are negative, so we add them not subtract them here
        adjustedStartingEquity = startingEquity + totalDeposits + totalWithdrawals
        returnPercentage = (endingEquity - adjustedStartingEquity) / adjustedStartingEquity
        return float('%.6f' % returnPercentage)

    except:
        ls.log.exception("alpha.getOneYearReturn")


def getOneYearBenchmarkReturn():
    try:
        benchmarkSymbol = os.getenv('BENCHMARK_SYMBOL')
        barsData = api.getStockBars(benchmarkSymbol, tk.todayMinus1Year, tk.nowMinus15Minutes).data[benchmarkSymbol]
        startingPrice = float(barsData[0].close)
        endingPrice = float(barsData[len(barsData)-1].close)
        benchmarkDividendYield = getDividendYield(benchmarkSymbol, endingPrice)
        returnPercentage = ((endingPrice - startingPrice) / startingPrice) + benchmarkDividendYield
        return float('%.6f' % returnPercentage)
    except:
        ls.log.exception("alpha.getOneYearBenchmarkReturn")


def getDividendYield(symbol, price):
    try:
        dividendHistory = api.getDividendHistory(symbol)
        annualDividend = 0
        for dividendPayment in dividendHistory:
            if dividendPayment['paymentDate'] >= tk.todayMinus1YearFormatted:
                annualDividend += dividendPayment['dividend']
            else:
                break
        dividendYield = annualDividend / price
        return dividendYield
    except:
        ls.log.exception("alpha.getDividendYield")


if __name__ == '__main__':
    try:
        main()
    except:
        ls.log.exception("alpha")
    finally:
        quit()

