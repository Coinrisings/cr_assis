import datetime
from cr_assis.eva.evaGateWallet import EvaGateWallet
from cr_assis.load import *
from cr_assis.load import datetime

class EvaBitGetMain(EvaGateWallet):
    def __init__(self):
        super().__init__()
        self.file_path = "/mnt/efs/fs1/data_ssh/mm/capital/bitget"
    
    def read_total_summary(self, start: datetime, end: datetime, is_play=True):
        super().read_total_summary(start, end, is_play = False)
        kline = self.get_btc_price(start, end).set_index("dt")
        self.total_summary = pd.merge(self.total_summary, kline[["close"]], left_index= True ,right_index= True, how="outer")
        self.draw_result_tabs(self.total_summary) if is_play else None