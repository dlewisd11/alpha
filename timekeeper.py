import datetime
import pytz
import logsetup as ls


try:
    now = datetime.datetime.now(pytz.timezone('US/Eastern'))
    today = now.today()
    formattedDate = today.strftime("%Y-%m-%d")
    yearMonthString = today.strftime("%Y_%m")
    currentTime = now.strftime("%H:%M")
except:
    ls.log.exception("timekeeper")