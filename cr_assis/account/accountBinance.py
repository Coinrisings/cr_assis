from cr_assis.account.accountOkex import AccountOkex
from cr_assis.account.accountBase import AccountBase
from cr_assis.connect.connectData import ConnectData
from pathlib import Path
import pandas as pd
import numpy as np
import ccxt, copy

class AccountBinance(AccountOkex):
    
    def __init__(self,deploy_id):
        
        super().__init__(deploy_id)
        
        self.exchange_position = "binance"
        self.exchange_combo = "binance"
        self.exchange_contract = "binance"
        self.folder = "pt"
        self.ccy = "USDT"
        self.principal_currency = "USDT"
        self.parameter: pd.DataFrame
        self.empty_position:pd.DataFrame = pd.DataFrame(columns = ["usdt", "usdt-swap", "usdt-future", "usd-swap", "usd-future", "busd-swap", "diff", "diff_U"])
        self.open_price: pd.DataFrame = pd.DataFrame(columns = ["usdt", "usdt-swap", "usdt-future", "usd-swap", "usd-future", "busd-swap"])
        self.now_price: pd.DataFrame = pd.DataFrame(columns = ["usdt", "usdt-swap", "usdt-future", "usd-swap", "usd-future", "busd-swap"])
        self.markets = ccxt.binance().load_markets()
        self.contractsize_uswap = {}
        self.cashBal = {}
        self.contractsize_cswap = {"BTC": 100, "ETH": 10, "BNB": 10, "LTC": 10, "DOGE": 10, "ETC": 10}
        self.exposure_number = 1
        self.is_master = {"usd-future":0, "usd-swap":1, "busd-swap":2, "usdt":3,"usdt-future":4, "usdt-swap":5,  "": np.inf}
        self.secret_id = {"usd-future": "@binance:futures_usd", "usd-swap": "@binance:swap_usd", "busd-swap": "@binance:swap_usdt",
                        "usdt": "@binance:spot", "usdt-future": "@binance:futures_usdt", "usdt-swap": "@binance:swap_usdt", "": ""}
        self.exchange_master, self.exchange_slave = "binance", "binance"
        self.path_orders = [f'{self.client}__{self.parameter_name}@binance_swap_usd', f'{self.client}__{self.parameter_name}@binance_swap_usdt']
        self.path_ledgers = [f'{self.client}__{self.parameter_name}@binance_swap_usd', f'{self.client}__{self.parameter_name}@binance_swap_usdt']
