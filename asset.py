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

            self.latestTradePrice = api.liveTradeData[self.symbol].price if liveTradeDataPresent else api.getLatestTrade(self.symbol).price
            self.secondaryPrice = api.getSecondaryPrice(self.symbol)
            ls.log.debug({'latestTradePrice': self.latestTradePrice, 'secondaryPrice': self.secondaryPrice})
            self.secondaryPrice = self.secondaryPrice if self.secondaryPrice is not None else self.latestTradePrice
            self.priceCheck = self.__priceCheck()
            self.currentPrice = float('%.2f' % (self.latestTradePrice if self.priceCheck else self.secondaryPrice))

            self.latestAsk = api.liveQuoteData[self.symbol].ask_price if liveQuoteDataPresent else self.latestQuote.ask_price
            self.latestBid = api.liveQuoteData[self.symbol].bid_price if liveQuoteDataPresent else self.latestQuote.bid_price
            ls.log.debug({'latestAsk': self.latestAsk, 'latestBid': self.latestBid})
            self.spreadCheck = self.__spreadCheck()
            self.latestAsk = self.latestAsk if self.spreadCheck else self.__getArtificialSpreadPrice('ask')
            self.latestBid = self.latestBid if self.spreadCheck else self.__getArtificialSpreadPrice('bid')

            self.limitPriceBuy = self.__getLimitPrice('buy')
            self.limitPriceSell = self.__getLimitPrice('sell')
            self.percentUpDown = self.__getPercentUpDown(atTheOpen)
            self.rsi = self.__getRSI(rsiPeriod, atTheOpen)

            logData = {
                        'symbol': self.symbol,
                        'currentPrice': self.currentPrice,
                        'percentUpDown': self.percentUpDown,
                        'rsiPeriod': rsiPeriod,
                        'rsi': self.rsi,
                        'limitPriceBuy': self.limitPriceBuy,
                        'limitPriceSell': self.limitPriceSell,
                        'latestAsk': self.latestAsk,
                        'latestBid': self.latestBid,
                        'previousOpeningPrice': self.previousOpeningPrice,
                        'previousClosingPrice': self.previousClosingPrice,
                        'spreadCheck': self.spreadCheck,
                        'priceCheck': self.priceCheck
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

            return float('%.3f' % rsi)
        except:
            ls.log.exception("Asset.__getRSI")


    def __getPercentUpDown(self, atTheOpen):
        try:
            previousPrice = self.previousOpeningPrice if atTheOpen else self.previousClosingPrice
            return float('%.6f' % ((self.currentPrice - previousPrice) / previousPrice))
        except:
            ls.log.exception("Asset.__getPercentUpDown")


    def __getLimitPrice(self, side):
        try:
            limitBuffer = float(os.getenv('LIMIT_BUFFER'))
            if side == 'buy':
                return float('%.2f' % (self.latestAsk * (1 + limitBuffer)))     
            else:
                return float('%.2f' % (self.latestBid * (1 - limitBuffer)))  
        except:
            ls.log.exception("Asset.__getLimitPrice")


    def __getArtificialSpreadPrice(self, side):
        try:
            spreadLimit = float(os.getenv('SPREAD_LIMIT'))
            if side == 'ask':
                return float('%.2f' % (self.currentPrice * (1 + spreadLimit)))     
            else:
                return float('%.2f' % (self.currentPrice * (1 - spreadLimit)))  
        except:
            ls.log.exception("Asset.__getArtificialAsk")


    def __spreadCheck(self):
        try:
            spreadPercentage = float(0)
            spreadLimit = float(os.getenv('SPREAD_LIMIT'))
            
            if self.latestBid != 0: 
                spreadPercentage = float(abs(((self.latestAsk / self.latestBid) - 1)))
            else:
                return False
                
            if spreadPercentage > spreadLimit:
                return False
            else:
                return True
        except:
            ls.log.exception("Asset.__spreadCheck")


    def __priceCheck(self):
        try:
            priceVariancePercentage = float(0)
            varianceLimit = float(os.getenv('PRICE_VARIANCE_LIMIT'))

            if self.secondaryPrice != 0:
                priceVariancePercentage = float(abs(((self.latestTradePrice / self.secondaryPrice) - 1)))
            else:
                return False
                
            if priceVariancePercentage > varianceLimit:
                return False
            else:
                return True
        except:
            ls.log.exception("Asset.__priceCheck")

