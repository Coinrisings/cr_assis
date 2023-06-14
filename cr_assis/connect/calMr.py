from cr_monitor.daily.dailyOkex import DailyOkex
from cr_assis.load import *
import datetime

daily = DailyOkex(ignore_test = True)
daily.mr_okex.price_range = [1]
ts = (datetime.datetime.utcnow() + datetime.timedelta(hours = 8)).strftime("%Y-%m-%d %H:%M:%S")
result = daily.get_account_mr()
data = pd.DataFrame.from_dict(daily.account_mr).rename(index={1: ts})
data.to_csv("/mnt/efs/fs1/data_ssh/cal_mr/okex.csv", mode = "a", header= (not os.path.isfile(f"/mnt/efs/fs1/data_ssh/cal_mr/okex.csv")))