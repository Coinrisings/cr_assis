from cr_assis.account.accountBase import AccountBase
from cr_assis.connect.connectData import ConnectData
import pandas as pd
import numpy as np
import datetime

class SsfoPnl(object):
    """pnl for ssf-o"""
    
    def __init__(self, accounts: list[AccountBase]) -> None:
        """
        Args:
            accounts (list): list of AccountBase
        """
        self.accounts = accounts
        self.database = ConnectData()
        self.now = datetime.datetime.utcnow()
        self.end_time = datetime.datetime.combine(((self.now + datetime.timedelta(hours = 8)) + datetime.timedelta(days = -1)).date(), datetime.datetime.max.time())
    
    def get_rpnl(self) -> dict:
        rpnl = {}
        third_pnl = {}
        for account in self.accounts:
            for day in [1, 3, 7]:
                third_pnl[day] = account.get_mean_equity() / account.get_mean_equity(the_time = f'now() - {day}d') - 1
            rpnl[account.parameter_name] = third_pnl.copy()
        self.rpnl = rpnl
        return rpnl

    def get_fpnl(self) -> dict:
        fpnl = {}
        ipnl = {}
        day_fpnl = {}
        day_ipnl = {}
        for account in self.accounts:
            account.get_equity() if not hasattr(account, "adjEq") else None
            account.end = self.end_time
            for day in [1, 3, 7]:
                account.start = self.end_time + datetime.timedelta(days = -day)
                account.get_ledgers()
                if len(account.ledgers) > 0:
                    account.get_fpnl()
                else:
                    account.fpnl = pd.DataFrame(columns = ["total"])
                day_fpnl[day] = account.fpnl["funding_fee"].sum() / account.adjEq if "funding_fee" in account.fpnl.columns else np.nan
                day_ipnl[day] = account.fpnl["interest"].sum() / account.adjEq if "interest" in account.fpnl.columns else 0
            fpnl[account.parameter_name] = day_fpnl.copy()
            ipnl[account.parameter_name] = day_ipnl.copy()
        self.fpnl = fpnl
        self.ipnl = ipnl
        return fpnl, ipnl