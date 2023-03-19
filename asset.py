import os
import logsetup as ls
import timekeeper as tk
import api

class Asset:

    def __init__(self, symbol, rsiPeriod):
        try:
            self.symbol = symbol            
            self.bars = self.__getBars()
            self.previousClosingPrice = self.__getPreviousClose()
            self.rsi = self.__getRSI(rsiPeriod)
            self.latestQuote = api.getLatestQuote(self.symbol)
            liveQuoteDataPresent = self.symbol in api.liveQuoteData
            liveTradeDataPresent = self.symbol in api.liveTradeData
            self.latestAsk = api.liveQuoteData[self.symbol].ask_price if liveQuoteDataPresent else self.latestQuote.ask_price
            self.latestBid = api.liveQuoteData[self.symbol].bid_price if liveQuoteDataPresent else self.latestQuote.bid_price
            self.latestTradePrice = api.liveTradeData[self.symbol].price if liveTradeDataPresent else api.getLatestTrade(self.symbol).price
            self.latestBarPrice = api.getStockLatestBar(self.symbol)[symbol].close
            self.secondaryPrice = api.getSecondaryPrice(self.symbol)
            self.averagePrice = self.__getAveragePrice()
            self.averagePriceBuy = self.__getAveragePriceBuy()
            self.averagePriceSell = self.__getAveragePriceSell()
            self.percentUpDown = self.__getPercentUpDown(self.previousClosingPrice, self.averagePrice)
            self.limitPriceBuy = self.__getLimitPrice(self.latestTradePrice if liveTradeDataPresent else self.averagePriceBuy, 'buy')
            self.limitPriceSell = self.__getLimitPrice(self.latestTradePrice if liveTradeDataPresent else self.averagePriceSell, 'sell')
            self.rsiMap = {
                            1: self.__getRSI(1),
                            2: self.__getRSI(2),
                            3: self.__getRSI(3),
                            4: self.__getRSI(4),
                            5: self.__getRSI(5),
                            6: self.__getRSI(6),
                            7: self.__getRSI(7),
                            8: self.__getRSI(8),
                            9: self.__getRSI(9),
                            10: self.__getRSI(10),
                            11: self.__getRSI(11),
                            12: self.__getRSI(12),
                            13: self.__getRSI(13),
                            14: self.__getRSI(14)
                          }

            logData = {
                        'symbol': self.symbol,
                        'previousClosingPrice': self.previousClosingPrice,
                        'rsi': self.rsi,
                        'rsiPeriod': rsiPeriod,
                        'latestAsk': self.latestAsk,
                        'latestBid': self.latestBid,
                        'latestTradePrice': self.latestTradePrice,
                        'latestBarPrice': self.latestBarPrice,
                        'secondaryPrice': self.secondaryPrice,
                        'averagePrice': self.averagePrice,
                        'averagePriceBuy': self.averagePriceBuy,
                        'averagePriceSell': self.averagePriceSell,
                        'limitPriceBuy': self.limitPriceBuy,
                        'limitPriceSell': self.limitPriceSell,
                        'percentUpDown': self.percentUpDown
            }

            ls.log.info(ls.prettyFormat(logData))

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


    def __getRSI(self, rsiPeriod):
        try:
            gain = 0
            loss = 0
            barsData = self.bars.data[self.symbol]

            for i in range(rsiPeriod):
                first = barsData[len(barsData)-2-i].close 
                second = barsData[len(barsData)-1-i].close
                
                if first<second:
                    gain = gain + abs(second-first)
                if first>second:
                    loss = loss + abs(second-first)
            
            averageGain = gain / rsiPeriod
            averageLoss = loss / rsiPeriod

            if averageLoss != 0:
                rs = averageGain / averageLoss
                rsi = 100 - (100 / (1 + rs))
            else:
                rsi = 100

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


    def __getAveragePrice(self):
        try:
            return (self.latestAsk + self.latestBid + self.latestBarPrice + self.latestTradePrice + self.secondaryPrice) / 5
        except:
            ls.log.exception("Asset.__getAveragePrice")


    def __getAveragePriceBuy(self):
        try:
            return (self.latestAsk + self.latestBarPrice + self.latestTradePrice + self.secondaryPrice) / 4
        except:
            ls.log.exception("Asset.__getAveragePriceBuy")


    def __getAveragePriceSell(self):
        try:
            return (self.latestBid + self.latestBarPrice + self.latestTradePrice + self.secondaryPrice) / 4
        except:
            ls.log.exception("Asset.__getAveragePriceSell")
