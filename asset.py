import os
import logsetup as ls
import timekeeper as tk
import api

class Asset:

    def __init__(self, symbol):
        try:
            self.symbol = symbol
            self.bars = self.__getBars()
            self.previousClosingPrice = self.__getPreviousClose()
            self.rsi = self.__getRSI()
            self.latestQuote = api.getLatestQuote(self.symbol)
            self.latestAsk = self.latestQuote.ask_price
            self.latestBid = self.latestQuote.bid_price
            self.latestTradePrice = api.getLatestTrade(self.symbol).price
            self.latestBarPrice = api.getStockLatestBar(self.symbol)[symbol].close
            self.percentUpDownBuy = self.__getPercentUpDown(self.previousClosingPrice, self.latestTradePrice)
            self.percentUpDownSell = self.__getPercentUpDown(self.previousClosingPrice, self.latestTradePrice)
            self.limitPriceBuy = self.__getLimitPrice(self.latestTradePrice, 'buy')
            self.limitPriceSell = self.__getLimitPrice(self.latestTradePrice, 'sell')

            logData = {
                        'symbol': self.symbol,
                        'previousClosingPrice': self.previousClosingPrice,
                        'rsi': self.rsi,
                        'latestAsk': self.latestAsk,
                        'latestBid': self.latestBid,
                        'latestTradePrice': self.latestTradePrice,
                        'latestBarPrice' : self.latestBarPrice,
                        'limitPriceBuy': self.limitPriceBuy,
                        'limitPriceSell': self.limitPriceSell,
                        'percentUpDownBuy': self.percentUpDownBuy,
                        'percentUpDownSell': self.percentUpDownSell
            }

            ls.log.info(logData)

        except:
            ls.log.exception("Asset.__init__")


    def __getBars(self):
        try:
            tradingCalendar = api.getTradingCalendar()
            stockBars = api.getStockBars(
                                    self.symbol, 
                                    tradingCalendar[0].close,
                                    tk.nowMinus15Minutes
            )
            return stockBars
        except:
            ls.log.exception("Asset.__getBars")


    def __getPreviousClose(self):
        try:
            barsData = self.bars.data[self.symbol]
            return barsData[len(barsData)-2].close
        except:
            ls.log.exception("Asset.__getPreviousClose")


    def __getRSI(self):
        try:
            gain = 0
            loss = 0
            barsData = self.bars.data[self.symbol]
            rsiPeriod = int(os.getenv('RSI_PERIOD'))

            for i in range(rsiPeriod):
                first = barsData[len(barsData)-2-i].close 
                second = barsData[len(barsData)-1-i].close
                
                if first<second:
                    gain = gain + abs(second-first)
                if first>second:
                    loss = loss + abs(second-first)
            
            averageGain = gain / rsiPeriod
            averageLoss = loss / rsiPeriod

            rs = averageGain / averageLoss
            rsi = 100 - 100 / (1 + rs)

            return rsi
        except:
            ls.log.exception("Asset.__getRSI")


    def __getPercentUpDown(self, previousPrice, currentPrice):
        try:
            return (currentPrice - previousPrice) / previousPrice
        except:
            ls.log.exception("Asset.__getPercentUpDown")


    def __getLimitPrice(self, currentPrice, side):
        try:
            limitBuffer = os.getenv('LIMIT_BUFFER')
            if side == 'buy':
                return float('%.2f' % (currentPrice * (1 + float(limitBuffer))))     
            else:
                return float('%.2f' % (currentPrice * (1 - float(limitBuffer))))  
        except:
            ls.log.exception("Asset.__getLimitPrice")

