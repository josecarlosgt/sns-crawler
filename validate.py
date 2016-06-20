import datetime

from validator.checkDegrees import CheckDegrees

today = datetime.date.today()
timeId = today.strftime('%d_%m_%Y')
checkDegrees = CheckDegrees(timeId)
checkDegrees.run()
