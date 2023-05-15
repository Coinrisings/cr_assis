import datetime, os
import pandas as pd
import numpy as np
from cr_assis.draw import draw_ssh
from bokeh.models.widgets import Panel, Tabs
from bokeh.plotting import figure,show
from bokeh.models import NumeralTickFormatter

class EvaGateWallet(object):
    
    def __init__(self):
        self.file_path = "/mnt/efs/fs1/data_ssh/mm/capital/gate"
        self.max_loss = 0.5
        self.max_mv = 0.2
        self.title_name = {"total_pnl": "所有子账户累计交易盈亏总额", "total_capital": "所有子账户子账户资金加总", "dnw_sum": "所有子账户累计转入-累计转出", "total_mv": "头寸大小统计"}
    
    def read_total_summary(self, start: datetime.datetime, end: datetime.datetime, is_play = True):
        total_summary = self.read_data(path = f"{self.file_path}/total", start = start, end = end)
        self.total_summary = total_summary
        self.draw_result_tabs(total_summary) if is_play else None
    
    def read_data(self, path: str, start: datetime.datetime, end: datetime.datetime) -> pd.DataFrame:
        data = pd.DataFrame()
        start_date = start.date() + datetime.timedelta(days = -1)
        end_date = end.date()
        date = start_date
        while date <= end_date:
            df = pd.read_csv(f"{path}/{date}.csv", index_col = 0) if os.path.isfile(f"{path}/{date}.csv") else pd.DataFrame()
            data = pd.concat([data, df])
            date += datetime.timedelta(days = 1)
        data.index = pd.to_datetime(data.index) + datetime.timedelta(hours = 8)
        data = data[(data.index >= start) & (data.index <= end)].copy()
        data.sort_index(inplace = True)
        return data
    
    def draw_result_tabs(self, result: pd.DataFrame):
        tabs = []
        for col in result.columns:
            p = draw_ssh.line(result[[col]], play = False)
            p.yaxis[0].formatter = NumeralTickFormatter(format="0,0") if col != "total_mv" else NumeralTickFormatter(format="0.0000%")
            tab = Panel(child = p, title = self.title_name[col])
            tabs.append(tab)
        t = Tabs(tabs = tabs)
        show(t)
    
    def print_equity(self, start: datetime.datetime, end: datetime.datetime):
        equity = self.read_data(path = f"{self.file_path}/subaccount/equity", start = start + datetime.timedelta(days = -3), end = end)
        self.single_equity = equity
        for ts in equity[(equity.index >= start) & (equity.index <= end)].index:
            max_equity = equity[(equity.index >= ts + datetime.timedelta(days = -3)) & (equity.index <= ts)].max()
            for uid in equity.columns:
                if max_equity[uid] > 0 and  max_equity[uid] / equity.loc[ts, uid] - 1 > self.max_loss:
                    print(f"uid {uid}账户在{ts}时刻净值{equity.loc[ts, uid]}相较于3天最高点{max_equity[uid]}回撤达到{self.max_loss}")
    
    def print_mv(self, start: datetime.datetime, end: datetime.datetime):
        mv = self.read_data(path = f"{self.file_path}/subaccount/mv", start = start, end = end)
        self.single_mv = mv
        for ts in mv.index:
            for uid in mv.columns:
                if mv.loc[ts, uid] > self.max_mv:
                    print(f"uid {uid}账户在{ts}时刻mv%{mv.loc[ts, uid]}过高超过{self.max_mv}")
    
    def print_subaccounts_situation(self, start: datetime.datetime, end: datetime.datetime):
        self.print_equity(start, end)
        self.print_mv(start, end)