import os
import uuid

import logsetup as ls
import timekeeper as tk
import database as db
import api

from dotenv import load_dotenv


def main():

    try:
        ls.log.info("BEGIN")

        if api.isMarketOpen():

            symbol = os.getenv('SYMBOL')
            previousClosingPrice = api.getPreviousClose(symbol)
            
            latestQuote = api.getLatestQuote(symbol)
            
            latestAsk = latestQuote.ask_price
            latestBid = latestQuote.bid_price

            percentUpDownBuy = getPercentUpDown(previousClosingPrice, latestAsk)
            percentUpDownSell = getPercentUpDown(previousClosingPrice, latestBid)

            limitPriceBuy = getLimitPrice(latestAsk, 'buy')
            limitPriceSell = getLimitPrice(latestBid, 'sell')

            buyEnabled = os.getenv('BUY_ENABLED') == 'True'
            sellEnabled = os.getenv('SELL_ENABLED') == 'True'

            #buy
            if buyEnabled and percentUpDownBuy <= 0:

                quantity = getOrderQuantity(symbol, limitPriceBuy)
                    
                if quantity > 0:
                    orderID = api.submitOrder(symbol, quantity, limitPriceBuy, 'buy')
                    orderFilled = api.orderFilled(orderID)
                    
                    if orderFilled:
                        purchasePrice = api.getFilledOrderAveragePrice(orderID)
                        insertBuyRecord(symbol, quantity, purchasePrice, orderID)

            #sell
            elif sellEnabled and percentUpDownSell > 0:

                positions = getOpenPositionsEligibleForSale(symbol, limitPriceSell)
                
                for position in positions:
                    tableRecordID = position[0]
                    symbol = position[1]
                    quantity = float(str(position[2]))
                    orderID = api.submitOrder(symbol, quantity, limitPriceSell, 'sell')
                    orderFilled = api.orderFilled(orderID)
                    
                    if orderFilled:
                        salePrice = api.getFilledOrderAveragePrice(orderID)
                        updateSoldPosition(tableRecordID, salePrice, orderID)

            
            logData = {
                        'symbol': symbol,
                        'previousClosingPrice': previousClosingPrice,
                        'latestAsk': latestAsk,
                        'latestBid': latestBid,
                        'limitPriceBuy': limitPriceBuy,
                        'limitPriceSell': limitPriceSell,
                        'percentUpDownBuy': percentUpDownBuy,
                        'percentUpDownSell': percentUpDownSell
            }

            ls.log.info(logData)            

    except:
        ls.log.exception("alpha.main")
    
    finally:
        ls.log.info("END")


def getPercentUpDown(previousPrice, currentPrice):
    try:
        return (currentPrice - previousPrice) / previousPrice
    except:
        ls.log.exception("alpha.getPercentUpDown")
        
        
def getLimitPrice(currentPrice, side):
    try:
        limitBuffer = os.getenv('LIMIT_BUFFER')
        if side == 'buy':
            return float('%.2f' % (currentPrice * (1 + float(limitBuffer))))     
        else:
            return float('%.2f' % (currentPrice * (1 - float(limitBuffer))))  
    except:
        ls.log.exception("alpha.getLimitPrice")


def getOrderQuantity(symbol, limitPrice):
    try:
        query = "SELECT COUNT(*) FROM " + db.dbTableName + " WHERE ISNULL(saleorderid) AND ISNULL(saledate) AND ISNULL(saleprice) AND symbol = %s"
        values = (symbol,)
        numberOpenPositions = db.runQueryAndReturnResults(query, values)[0][0]

        enduranceDays = int(os.getenv('ENDURANCE_DAYS'))
        enduranceDaysRemaining = enduranceDays - numberOpenPositions

        allowMarginTrading = os.getenv('ALLOW_MARGIN_TRADING') == 'True'
        accountInformation = api.getAccountInformation()

        if allowMarginTrading:
            activePercentageBuyingPower = float(os.getenv('ACTIVE_PERCENTAGE_BUYING_POWER'))
            buyingPower = float(accountInformation.buying_power)
            activeBuyingPower = buyingPower * activePercentageBuyingPower
        
        else:
            activeBuyingPower = float(accountInformation.cash)

        if(enduranceDaysRemaining > 0):
            orderQuantity = int((activeBuyingPower / enduranceDaysRemaining) / limitPrice)
            return orderQuantity
        else:
            orderQuantity = int(activeBuyingPower / limitPrice)
            return orderQuantity
    except:
        ls.log.exception("alpha.getOrderQuantity")


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


def getOpenPositionsEligibleForSale(symbol, limitPriceSell):
    try:
        sellSideMarginMinimum = float(os.getenv('SELL_SIDE_MARGIN_MINIMUM'))
        marginInterestRate = float(os.getenv('MARGIN_INTEREST_RATE'))
                
        query = "SELECT id, symbol, quantity FROM " + db.dbTableName + " WHERE ISNULL(saleorderid) AND ISNULL(saledate) AND ISNULL(saleprice) AND symbol = %s AND purchasedate < %s AND ((%s / purchaseprice) - 1) >= (%s + (DATEDIFF(%s, purchasedate) * (%s / 360)))"
        values = (symbol, tk.formattedDate, limitPriceSell, sellSideMarginMinimum, tk.formattedDate, marginInterestRate)
        positions = db.runQueryAndReturnResults(query, values)
        return positions
    except:
        ls.log.exception("alpha.getOpenPositionsEligibleForSale")


if __name__ == '__main__':
    try:
        load_dotenv()
        main()
    except:
        ls.log.exception("alpha")
