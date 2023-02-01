from datetime import datetime
from datetime import timedelta
import pytz
import logsetup as ls


try:
    currentDateTime = datetime.now(pytz.timezone('America/New_York'))
    currentDate = currentDateTime.today()
    nowMinus15Minutes = currentDateTime - timedelta(minutes=15)
    todayMinus30Days = currentDateTime - timedelta(days=30)
    todayMinus1Year = currentDateTime - timedelta(days=365)
    todayMinus1YearFormatted = todayMinus1Year.strftime("%Y-%m-%d")
    formattedDate = currentDateTime.strftime("%Y-%m-%d")
    yearMonthString = currentDateTime.strftime("%Y_%m")
    currentTime = currentDateTime.strftime("%H:%M")
    weekDay = currentDateTime.weekday()
    hour = currentDateTime.hour


    def stringToDate(dateString):
        try:
            return datetime.strptime(dateString, "%Y-%m-%d")
        except:
            ls.log.exception("timekeeper.stringToDate")


    def dateDiff(date1, date2):
        try:
            return abs((date2-date1).days)
        except:
            ls.log.exception("timekeeper.dateDiff")

            
except:
    ls.log.exception("timekeeper")