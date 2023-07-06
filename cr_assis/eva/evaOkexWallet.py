from .evaGateWallet import EvaGateWallet
import os, datetime, yaml, copy
from cr_assis.load import *
from cr_assis.draw import draw_ssh
from bokeh.plotting import figure,show
from bokeh.models import NumeralTickFormatter
from bokeh.models.widgets import Panel, Tabs

class EvaOkexWallet(EvaGateWallet):
    
    def __init__(self):
        self.file_path = "/mnt/efs/fs1/data_ssh/mm/okex/total" if os.path.exists("/mnt/efs/fs1/data_ssh/mm/okex/total") else os.environ["HOME"] + "/data/mm/okex/total"
        self.get_accounts()
    
    def get_accounts(self) -> None:
        with open(f"{os.environ['HOME']}/.cr_assis/account_okex_api.yml", "rb") as f:
            data: list[dict] = yaml.load(f, Loader= yaml.SafeLoader)
        self.accounts = [i['name'] for i in data if "hf" == i['name'].split("_")[0]]
    
    def read_total_summary(self, start: datetime.datetime, end: datetime.datetime, accounts = [],is_play = True):
        accounts = self.accounts if accounts == [] else accounts
        tabs = []
        accounts_panle = {}
        for account in accounts:
            total_summary = self.read_data(path = f"{self.file_path}/{account}", start = start, end = end)
            self.total_summary = total_summary.drop("position_value", axis = 1) if "position_value" in total_summary.columns else total_summary
            accounts_panle[account] = draw_ssh.line_doubleY(self.total_summary, right_columns=["mv%"], play = False) if is_play and len(self.total_summary) > 0 else None
            kline = self.get_btc_price(start, end)
            kline.set_index("dt", inplace = True)
            accounts_panle[account].extra_y_ranges['y3'] = Range1d(start = min(kline["open"].astype(float).values), end = max(kline["open"].astype(float).values))
            accounts_panle[account].add_layout(LinearAxis(y_range_name = 'y3'),'right')
            accounts_panle[account].line(kline.index, kline["open"], legend_label="kline", line_color="green",name = "kline", y_range_name='y3', line_width = 2)
            if accounts_panle[account] != None:
                accounts_panle[account].yaxis[0].formatter = NumeralTickFormatter(format="0,0")
                accounts_panle[account].yaxis[1].formatter = NumeralTickFormatter(format="0.0000%")
                accounts_panle[account].yaxis[2].formatter = NumeralTickFormatter(format="0.00")
                tab = Panel(child = accounts_panle[account], title = account)
                tabs.append(tab)
        if len(tabs) > 0:
            t = Tabs(tabs = tabs)
            show(t)
