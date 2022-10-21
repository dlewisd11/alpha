from datetime import datetime
from datetime import timedelta
import pytz
import logsetup as ls


try:
    now = datetime.now(pytz.timezone('US/Eastern'))
    nowMinus15Minutes = now - timedelta(minutes=15)
    today = now.today()
    todayMinus30Days = today - timedelta(days=30)
    formattedDate = today.strftime("%Y-%m-%d")
    yearMonthString = today.strftime("%Y_%m")
    currentTime = now.strftime("%H:%M")


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