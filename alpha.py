import os
import mysql.connector
import alpaca_trade_api as tradeapi
import datetime
import pytz
import uuid

from dotenv import load_dotenv
from alpaca_trade_api import TimeFrame
from datetime import timedelta
from time import sleep


load_dotenv()

db = mysql.connector.connect(
    host=os.getenv('DB_HOST'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PW'),
    database=os.getenv('DB_NAME')
)

api = tradeapi.REST(
    key_id = os.getenv('API_KEY_ID'),
    secret_key = os.getenv('SECRET_KEY'),
    base_url = os.getenv('ENDPOINT')
)

dbTableName = str(os.getenv('DB_TABLE_NAME'))

logFile = open(os.getenv('LOG_FILE_NAME'), "a")

now = datetime.datetime.now(pytz.timezone('US/Eastern'))
today = now.today()
formattedDate = today.strftime("%Y-%m-%d")
currentTime = now.strftime("%H:%M")

logFile.write("\n--------------------------------\n")
logFile.write(str(now) + "\n")


################################################################################


def isMarketOpen():
    return api.get_clock()._raw['is_open']


def buy(symbol, quantity, limitPrice):

    orderID = api.submit_order(
                symbol=symbol,
                qty=quantity,
                side='buy',
                type='limit',
                limit_price=limitPrice,
                time_in_force='day'
    )._raw['id']

    logFile.write(str("buy order submitted") + "\n")

    if orderFilled(orderID):
        purchasePrice = api.get_order(orderID)._raw['filled_avg_price']
        tableRecordID = str(uuid.uuid4())
        query = "INSERT INTO " + dbTableName + " (id, symbol, quantity, purchasedate, purchaseprice, purchaseorderid) VALUES (%s, %s, %s, %s, %s, %s)"
        values = (tableRecordID, symbol, quantity, formattedDate, purchasePrice, orderID)
        runQuery(query, values)
        logFile.write(str("buy order filled") + "\n")
    else:
        api.cancel_order(orderID)
        logFile.write(str("buy order cancelled") + "\n")
    

def sell(symbol, quantity, limitPrice, tableRecordID):
    orderID = api.submit_order(
                symbol=symbol,
                qty=quantity,
                side='sell',
                type='limit',
                limit_price=limitPrice,
                time_in_force='day'
    )._raw['id']

    logFile.write(str("sell order submitted") + "\n")

    if orderFilled(orderID):
        salePrice = api.get_order(orderID)._raw['filled_avg_price']
        query = "UPDATE " + dbTableName + " SET saledate = %s, saleprice = %s, saleorderid = %s WHERE id = %s"
        values = (formattedDate, salePrice, orderID, tableRecordID)
        runQuery(query, values)
        logFile.write(str("sell order filled") + "\n")
    else:
        api.cancel_order(orderID)
        logFile.write(str("sell order cancelled") + "\n")


def runQuery(query, values):
    dbCursor = db.cursor()
    dbCursor.execute(query, values)
    db.commit()


def runQueryAndReturnResults(query, values):
    dbCursor = db.cursor()
    dbCursor.execute(query, values)
    return dbCursor.fetchall()
    

def orderFilled(orderID):
    orderFilled = False
    for i in range(int(os.getenv('ORDER_WAIT_ITERATIONS'))):
        orderFilled = api.get_order(orderID)._raw['status'] == 'filled'
        if orderFilled:
            break
        sleep(int(os.getenv('ORDER_WAIT_SECONDS')))
    return orderFilled


def getPreviousClose(symbol):
    calendarStartDate = today - timedelta(days=14)
    tradingCalendar = api.get_calendar(calendarStartDate, today)
    previousTradingDate = tradingCalendar[len(tradingCalendar)-2]._raw['date']
    return api.get_bars(
        'VT',
        TimeFrame.Day,
        previousTradingDate,
        previousTradingDate
    )[0]._raw['c']


def getLatestPrice(symbol):
    return api.get_latest_bar(symbol)._raw['c']


def getPercentUpDown(previousPrice, currentPrice):
    return (currentPrice - previousPrice) / previousPrice
    
    
def getLimitPrice(currentPrice, side):
    limitBuffer = os.getenv('LIMIT_BUFFER')
    if side == 'buy':
        return float('%.2f' % (currentPrice * (1 + float(limitBuffer))))     
    else:
        return float('%.2f' % (currentPrice * (1 - float(limitBuffer))))


def getOrderQuantity(symbol, limitPrice):

    query = "SELECT COUNT(*) FROM " + dbTableName + " WHERE ISNULL(saleorderid) AND ISNULL(saledate) AND ISNULL(saleprice) AND symbol = %s"
    values = (symbol,)
    numberOpenPositions = runQueryAndReturnResults(query, values)[0][0]

    enduranceDays = int(os.getenv('ENDURANCE_DAYS'))
    enduranceDaysRemaining = enduranceDays - numberOpenPositions

    activePercentageBuyingPower = float(os.getenv('ACTIVE_PERCENTAGE_BUYING_POWER'))
    adjustedBuyingPower = float(api.get_account()._raw['buying_power']) * activePercentageBuyingPower

    if(enduranceDaysRemaining > 0):
        orderQuantity = int((adjustedBuyingPower / enduranceDaysRemaining) / limitPrice)
        return orderQuantity
    else:
        orderQuantity = int(adjustedBuyingPower / limitPrice)
        return orderQuantity


################################################################################


if isMarketOpen():
    symbol = os.getenv('SYMBOL')
    previousClosingPrice = getPreviousClose(symbol)
    currentPrice = getLatestPrice(symbol)
    percentUpDown = getPercentUpDown(previousClosingPrice, currentPrice)

    #buy
    if percentUpDown <= 0:
        limitPrice = getLimitPrice(currentPrice, 'buy')
        quantity = getOrderQuantity(symbol, limitPrice)
        
        if quantity > 0:
            buy(symbol, quantity, limitPrice)

        logData = {
                    'symbol': symbol,
                    'previousClose': previousClosingPrice,
                    'currentPrice': currentPrice,
                    'limitPrice': limitPrice,
                    'percentUpDown': percentUpDown
        }

    #sell
    elif percentUpDown > 0:
        limitPrice = getLimitPrice(currentPrice, 'sell')
        sellSideMarginMinimum = float(os.getenv('SELL_SIDE_MARGIN_MINIMUM'))
        marginInterestRate = float(os.getenv('MARGIN_INTEREST_RATE'))
        
        query = "SELECT id, symbol, quantity FROM " + dbTableName + " WHERE ISNULL(saleorderid) AND ISNULL(saledate) AND ISNULL(saleprice) AND symbol = %s AND purchasedate < %s AND ((%s / purchaseprice) - 1) >= (%s + (DATEDIFF(%s, purchasedate) * (%s / 360)))"
        values = (symbol, formattedDate, limitPrice, sellSideMarginMinimum, formattedDate, marginInterestRate)
        positions = runQueryAndReturnResults(query, values)
        
        for position in positions:
            tableRecordID = position[0]
            symbol = position[1]
            quantity = float(str(position[2]))
            sell(symbol, quantity, limitPrice, tableRecordID)

        logData = {
                    'symbol': symbol,
                    'previousClose': previousClosingPrice,
                    'currentPrice': currentPrice,
                    'limitPrice': limitPrice,
                    'percentUpDown': percentUpDown
        }

    logFile.write(str(logData) + "\n")

logFile.close()
