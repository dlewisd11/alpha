import os
import logsetup as ls
import timekeeper as tk
import api

class Asset:

    def __init__(self, symbol, rsiPeriod, atTheOpen):
        try:
            self.symbol = symbol            
            self.bars = self.__getBars()
            self.previousOpeningPrice = self.__getPreviousOpen()
            self.previousClosingPrice = self.__getPreviousClose()            
            self.latestQuote = api.getLatestQuote(self.symbol)
            liveQuoteDataPresent = self.symbol in api.liveQuoteData
            liveTradeDataPresent = self.symbol in api.liveTradeData
            self.latestAsk = api.liveQuoteData[self.symbol].ask_price if liveQuoteDataPresent else self.latestQuote.ask_price
            self.latestBid = api.liveQuoteData[self.symbol].bid_price if liveQuoteDataPresent else self.latestQuote.bid_price
            self.latestTradePrice = api.liveTradeData[self.symbol].price if liveTradeDataPresent else api.getLatestTrade(self.symbol).price
            self.secondaryPrice = api.getSecondaryPrice(self.symbol)
            self.secondaryPrice = self.secondaryPrice if self.secondaryPrice is not None else self.latestTradePrice
            self.spreadCheck = self.__spreadCheck()
            self.currentPrice = self.latestTradePrice if self.spreadCheck else self.secondaryPrice
            self.limitPriceBuy = self.__getLimitPrice(self.latestAsk if self.spreadCheck else self.secondaryPrice, 'buy')
            self.limitPriceSell = self.__getLimitPrice(self.latestBid if self.spreadCheck else self.secondaryPrice, 'sell')
            self.percentUpDown = self.__getPercentUpDown(self.previousOpeningPrice if atTheOpen else self.previousClosingPrice, self.currentPrice)
            self.rsi = self.__getRSI(rsiPeriod, atTheOpen)

            logData = {
                        'symbol': self.symbol,
                        'limitPriceBuy': self.limitPriceBuy,
                        'limitPriceSell': self.limitPriceSell,
                        'percentUpDown': self.percentUpDown,
                        'rsi': self.rsi,
                        'rsiPeriod': rsiPeriod,
                        'latestAsk': self.latestAsk,
                        'latestBid': self.latestBid,
                        'spreadCheck': self.spreadCheck,
                        'previousOpeningPrice': self.previousOpeningPrice,
                        'previousClosingPrice': self.previousClosingPrice,
                        'latestTradePrice': self.latestTradePrice,
                        'secondaryPrice': self.secondaryPrice
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


    def __getPreviousOpen(self):
        try:
            barsData = self.bars.data[self.symbol]
            return barsData[len(barsData)-2].open
        except:
            ls.log.exception("Asset.__getPreviousOpen")


    def __getPreviousClose(self):
        try:
            barsData = self.bars.data[self.symbol]
            return barsData[len(barsData)-2].close
        except:
            ls.log.exception("Asset.__getPreviousClose")


    def __getRSI(self, rsiPeriod, atTheOpen):
        try:
            gain = 0
            loss = 0
            barsData = self.bars.data[self.symbol]

            for i in range(rsiPeriod):
                first = barsData[len(barsData)-2-i].open if atTheOpen else barsData[len(barsData)-2-i].close
                
                if i == 0:
                    second = self.currentPrice
                elif atTheOpen and i != 0:
                    second = barsData[len(barsData)-1-i].open
                else:
                    second = barsData[len(barsData)-1-i].close

                if first<second:
                    gain = gain + abs(second-first)
                if first>second:
                    loss = loss + abs(second-first)
            
            averageGain = gain / rsiPeriod
            averageLoss = loss / rsiPeriod

            if averageLoss != 0:
                rs = averageGain / averageLoss
                rsi = 100 - 100 / (1 + rs)
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


    def __spreadCheck(self):
        try:
            spreadPercentage = float(0)
            priceVariancePercentage = float(0)
            spreadLimit = float(os.getenv('SPREAD_LIMIT'))
            
            if self.latestBid != 0: 
                spreadPercentage = float(abs(((self.latestAsk / self.latestBid) - 1)))
            else:
                return False

            if self.secondaryPrice != 0:
                priceVariancePercentage = float(abs(((self.latestTradePrice / self.secondaryPrice) - 1)))
            else:
                return False
                
            if spreadPercentage > spreadLimit or priceVariancePercentage > spreadLimit:
                return False
            else:
                return True
        except:
            ls.log.exception("Asset.__spreadCheck")

