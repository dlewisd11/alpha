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

logFile.write(str(now) + "\n")


################################################################################


def isMarketOpen():
    return api.get_clock()._raw['is_open']


def allowOrderBasedOnCost(orderCost):
    accountBalance = api.get_account()._raw['cash']
    logFile.write(str({'accountBalance': accountBalance}) + "\n")
    return float(accountBalance) > orderCost


def buy(symbol, quantity, limitPrice):

    orderID = api.submit_order(
                symbol=symbol,
                qty=quantity,
                side='buy',
                type='limit',
                limit_price=limitPrice,
                time_in_force='day'
    )._raw['id']

    if orderFilled(orderID):
        purchasePrice = api.get_order(orderID)._raw['filled_avg_price']
        tableRecordID = str(uuid.uuid4())
        query = "INSERT INTO " + dbTableName + " (id, symbol, quantity, purchasedate, purchaseprice, purchaseorderid) VALUES (%s, %s, %s, %s, %s, %s)"
        values = (tableRecordID, symbol, quantity, formattedDate, purchasePrice, orderID)
        runQuery(query, values)
    else:
        api.cancel_order(orderID)
    

def sell(symbol, quantity, limitPrice, tableRecordID):
    orderID = api.submit_order(
                symbol=symbol,
                qty=quantity,
                side='sell',
                type='limit',
                limit_price=limitPrice,
                time_in_force='day'
    )._raw['id']

    if orderFilled(orderID):
        salePrice = api.get_order(orderID)._raw['filled_avg_price']
        query = "UPDATE " + dbTableName + " SET saledate = %s, saleprice = %s, saleorderid = %s WHERE id = %s"
        values = (formattedDate, salePrice, orderID, tableRecordID)
        runQuery(query, values)
    else:
        api.cancel_order(orderID)


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


################################################################################


if isMarketOpen():
    symbol = os.getenv('SYMBOL')
    orderQuantity = os.getenv('ORDER_QUANTITY')
    previousClosingPrice = getPreviousClose(symbol)
    currentPrice = getLatestPrice(symbol)
    percentUpDown = getPercentUpDown(previousClosingPrice, currentPrice)

    #buy
    if percentUpDown < 0:
        limitPrice = getLimitPrice(currentPrice, 'buy')
        orderCost = float(orderQuantity) * limitPrice
        
        if allowOrderBasedOnCost(orderCost):
            buy(symbol, orderQuantity, limitPrice)

    #sell
    elif percentUpDown > 0:
        limitPrice = getLimitPrice(currentPrice, 'sell')
        
        query = "SELECT id, quantity FROM " + dbTableName + " WHERE ISNULL(saleorderid) AND ISNULL(saledate) AND ISNULL(saleprice) AND symbol = %s AND purchasedate < %s AND purchaseprice < %s"
        values = (symbol, formattedDate, limitPrice)
        positions = runQueryAndReturnResults(query, values)
        
        for position in positions:
            tableRecordID = position[0]
            quantity = float(str(position[1]))
            sell(symbol, quantity, limitPrice, tableRecordID)

logFile.close()
