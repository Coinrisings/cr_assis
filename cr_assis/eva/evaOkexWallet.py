from .evaGateWallet import EvaGateWallet
import os, datetime, yaml
from cr_assis.draw import draw_ssh
from bokeh.plotting import figure,show
from bokeh.models import NumeralTickFormatter
from bokeh.models.widgets import Panel, Tabs

class EvaOkexWallet(EvaGateWallet):
    
    def __init__(self):
        self.file_path = "/mnt/efs/fs1/data_ssh/mm/okex/total" if os.path.exists("/mnt/efs/fs1/data_ssh/mm/okex") else os.environ["HOME"] + "/data/mm/okex/total"
        self.get_accounts()
    
    def get_accounts(self) -> None:
        with open(f"{os.environ['HOME']}/.cr_assis/account_okex_api.yml", "rb") as f:
            data: list[dict] = yaml.load(f, Loader= yaml.SafeLoader)
        self.accounts = [i['name'] for i in data if "hf" == i['name'].split("_")[0]]
    
    def read_total_summary(self, start: datetime.datetime, end: datetime.datetime, accounts = [],is_play = True):
        accounts = self.accounts if accounts == [] else accounts
        tabs = []
        for account in accounts:
            total_summary = self.read_data(path = f"{self.file_path}/{account}", start = start, end = end)
            self.total_summary = total_summary.drop("position_value", axis = 1) if "position_value" in total_summary.columns else total_summary
            p = draw_ssh.line_doubleY(self.total_summary, right_columns=["mv%"], play = False) if is_play and len(self.total_summary) > 0 else None
            p.yaxis[0].formatter = NumeralTickFormatter(format="0,0")
            tab = Panel(child = p, title = account)
            tabs.append(tab)
        t = Tabs(tabs = tabs)
        show(t)