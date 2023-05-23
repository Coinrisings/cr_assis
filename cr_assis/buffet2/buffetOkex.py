from cr_assis.buffet2.buffet2 import Get_Parameter as Buffet
from cr_assis.account.accountOkex import AccountOkex
from cr_assis.account.initAccounts import InitAccounts
from cr_assis.connect.connectData import ConnectData
import pandas as pd
import numpy as np
import os, datetime, ccxt

class BuffetOkex(Buffet):
    def __init__(self):
        super().__init__()
        self.database = ConnectData()
        self.markets = ccxt.okex().load_markets()
        self.accounts: dict[str, AccountOkex]
        self.coin_price: dict[str, float] = {}
        self.usd_contractsize: dict[str, float] = {}
        self.spreads: dict[str, dict[str, pd.DataFrame]] = {}
        self.folder = "dt"
        self.exchange_position = "okexv5"
        self.load_default_config()
        self.contractsize_path: str = os.environ['HOME'] + '/parameters/config_buffet/dt/contractsize.csv'
        self.parameter_cols = ['account', 'coin', 'portfolio', 'open', 'closemaker', 'position', 'closetaker', 'open2', 'closemaker2', 'position2', 'closetaker2', 'fragment',
                            'fragment_min', 'funding_stop_open', 'funding_stop_close', 'position_multiple', 'timestamp', 'is_long', 'chase_tick', 'master_pair', 'slave_pair', "master_secret", "slave_secret", "combo"]
    
    def load_default_config(self):
        self.account_config = {} if not hasattr(self, "account_config") else self.account_config
        self.account_config["default"] = {
            "funding_stop_open":0.0001,
            "funding_stop_close":0.005,
            "chase_tick":1,
            "fragment_min_u":10,
            "fragment_u":2000,
            "position_multiple":1,
            "future_date":[""],
            "open":[],
            "closemaker":[1.005],
            "closemaker2":[],
            "closetaker":[],
            "closetaker2":[],
            "maxloss":0,
            "cm2_chg":0.0003,
            "close_thresh":30,
            "open_thresh":30,
            "cm2_chg":0.0003,
            "open_add":0,
            "close_add":0}
    
    def get_contractsize(self, symbol: str) -> float:
        ret = self.markets[symbol]["contractSize"] if symbol in self.markets.keys() else np.nan
        self.usd_contractsize[symbol.split("/")[0].upper()] = ret
        return ret
    
    def get_contractsize_cswap(self, coin: str) ->float:
        
        symbol = f"{coin}/USD:{coin}"
        contractsize = self.get_contractsize(symbol)
        self.usd_contractsize[coin] = contractsize
        return contractsize
    
    def get_usd_contractsize(self, coin: str) -> float:
        coin = coin.upper()
        ret = self.usd_contractsize[coin] if coin in self.usd_contractsize.keys() else self.get_contractsize(symbol = f"{coin}/USD:{coin}")
        return ret
    
    def get_redis_price(self, coin: str) -> float:
        ret = self.database.get_redis_data(key = f"{self.exchange_position}/{coin.lower()}-usdt")
        price = float(ret[b'ask0_price']) if b'ask0_price' in ret.keys() else np.nan
        self.coin_price[coin.upper()] = price
        return price
    
    def get_coin_price(self, coin: str) -> float:
        coin = coin.upper()
        ret = self.coin_price[coin] if coin in self.coin_price.keys() else self.get_redis_price(coin)
        return ret
    
    def check_account(self, account: AccountOkex) -> bool:
        account.get_account_position()
        account.get_mgnRatio()
        is_nan = False
        # 权益获取，获取不到跳过
        if not hasattr(account, "adjEq") or np.isnan(account.adjEq):
            self.logger.warning(f"{account.parameter_name}:获取equity错误!")
            is_nan = True
        # 仓位获取，获取不到跳过
        if not hasattr(account, "position") or not hasattr(account, "now_position") or account.now_position["is_exposure"].sum() >0:
            self.logger.warning(f"{account.parameter_name}:最近10分钟position数据缺失或者有敞口")
            is_nan = True
        # mr获取，获取不到跳过
        if not hasattr(account, "mr") or len(account.mr) == 0:
            self.logger.warning(f"{account.parameter_name}:没有获取到当前的mr。")
            is_nan = True
        return is_nan
    
    def init_parameter(self, now_pos: pd.DataFrame, account: AccountOkex) -> pd.DataFrame:
        """初始化parameter
        """
        parameter = pd.DataFrame(columns=self.parameter_cols, index = now_pos.index)
        # 如果账户持有，赋值为原来参数
        if len(now_pos) > 0:
            self.logger.info(f"{account.parameter_name}:非新账户，初始化目前已经持有币的parameter参数。")
            parameter['portfolio'] = 0
            cols = ["position", "master_pair", "slave_pair", "master_secret", "slave_secret", "combo"]
            parameter[cols] = now_pos[cols]
            parameter['is_long'] = now_pos['side'].apply(lambda x: 1 if x == 'long' else 0)
        # 新账户，所有参数默认为空
        else:
            self.logger.info(f"{account.parameter_name}:新账户，初始化parameter，参数默认为空。")
        return parameter
    
    def handle_position(self, account: AccountOkex) -> pd.DataFrame:
        coins = account.position["coin"].unique()
        now_pos = account.position.copy().set_index("coin")
        for coin in coins:
            combo = now_pos.loc[coin, "combo"]
            now_pos.drop(coin, inplace= True) if combo in self.account_config.keys() and now_pos.loc[coin, "MV"] < self.account_config[combo]['min_select_u']\
                and now_pos.loc[coin, "MV%"] < self.account_config[combo]['min_select_ratio'] else None
        account.position = now_pos.copy()
        return now_pos
    
    def init_accounts(self) -> dict[str, AccountOkex]:
        """初始化所有orch打开的账户 删除获取不到position、adjEq和mr的账户
        """
        init = InitAccounts(ignore_test=False)
        self.accounts = init.init_accounts_okex()
        self.load_default_config()
        names = set(self.accounts.keys())
        for name in names:
            self.accounts.pop(name, None) if self.check_account(account = self.accounts[name]) else None
        return self.accounts
    
    def get_folders(self):
        """整理所有的github上传路径, 并将上传路径相同的账户归类
        """
        self.folders: set[str] = set()
        self.git_folder : dict[str, set[AccountOkex]] = {}
        self.init_accounts() if not hasattr(self, "accounts") else None
        for account in self.accounts.values():
            folder = account.folder
            self.folders.add(folder)
            self.git_folder[folder] = (self.git_folder[folder] | set([account])) if folder in self.git_folder.keys() else set([account])
    
    def get_all_names(self) -> set[str]:
        names = set()
        for combo in self.account_config.keys():
            names = names | set(self.account_config[combo]["accounts"]) if combo != "default" else names
        names = names & set(self.accounts.keys())
        return names
    
    def total_mv_reduce(self, account: AccountOkex, combo: str) -> pd.DataFrame:
        """总MV%超限减仓
        Returns:
            pd.DataFrame: parameter
        """
        now_pos = account.position[account.position["combo"] == combo].copy()
        now_mv = now_pos["MV%"].sum()
        config = self.account_config[combo]
        parameter = account.parameter
        mv_plus = now_mv - config['accounts'][account.parameter_name][0] * config['reduce_mv_multiple']
        self.logger.info(f"{account.parameter_name}:总仓位为：{now_mv} 大于 {config['accounts'][account.parameter_name][0] * config['max_mv_multiple']}，进行总仓位超限减仓，{mv_plus}%的仓位将会被减掉。")
        hold_coin = [i for i in list(dict(sorted(config["add"].items(), key=lambda x: x[1])).keys()) if i in now_pos.index]
        for c in hold_coin:
            if mv_plus > 0:
                ava_reduce = now_pos.loc[c, 'MV%']
                if ava_reduce <= mv_plus:
                    parameter.loc[c, ['portfolio', 'position']] = [-1, 0]
                    self.logger.info(f"{account.parameter_name}:{c}目前mv为{ava_reduce}%，小于{mv_plus}%,该币将会被一档减仓至0")
                else:
                    parameter.loc[c, ['portfolio', 'position']] = [-2, parameter.loc[c, "position"] * (ava_reduce - mv_plus) / ava_reduce]
                    self.logger.info(f"{account.parameter_name}:{c}目前mv为{ava_reduce}%，该币将会被二档减仓至{ava_reduce - mv_plus}%。")
                mv_plus -= ava_reduce
            else:
                break
        return parameter
    
    def check_add(self, account: AccountOkex, combo: str) -> bool:
        """检查是否能加仓
        """
        config = self.account_config[combo]
        add= config['add']
        now_mv = account.position[account.position["combo"] == combo]["MV%"].sum()
        is_add = False
        if add == {} or sum(add.values()) == 0:
            self.logger.warning(f"{account.parameter_name}:可选加仓币数为{add},不执行加仓操作。")
        elif min(account.mr.values()) < config['accounts'][account.parameter_name][1]:
            self.logger.warning(f"{account.parameter_name}:目前账户有合约对应的mr低于{config['accounts'][account.parameter_name][1]},不执行加仓操作。")
        elif now_mv >= config['accounts'][account.parameter_name][0]:
            self.logger.info(f"{account.parameter_name}:剩余可加仓mv为：0%，不执行加仓操作。")
        else:
            self.logger.info(f"""{account.parameter_name}:剩余可加仓mv为：{max(0, config["accounts"][account.parameter_name][0] - now_mv)}%，执行加仓操作。""")
            is_add = True
        return is_add
        
    def add_mv(self, account: AccountOkex, combo: str) -> pd.DataFrame:
        """加仓
        """
        parameter = account.parameter
        now_pos = account.position[account.position["combo"] == combo].copy()
        res_mv = max(0.0001, self.account_config[combo]["accounts"][account.parameter_name][0] - now_pos["MV%"].sum())
        for coin in self.account_config[combo]["add"].keys():
            hold_coin_mv = now_pos.loc[coin, 'MV%'] if coin in now_pos.index else 0
            coin_ava_mv = 0
            if hold_coin_mv > self.account_config[combo]['signal_uplimit_mv'][coin]:
                self.logger.info(f"{account.parameter_name}:{coin},'该币单币上限超限，该币加仓跳过")
            elif coin in parameter.index and parameter.loc[coin, "combo"] != combo:
                self.logger.info(f"{account.parameter_name}-{combo}:{coin}该币在{parameter.loc[coin, 'combo']}有持仓了，不在{combo}进行加仓操作")
            else:
                coin_ava_mv = min(self.account_config[combo]['signal_uplimit_mv'][coin] - hold_coin_mv,res_mv * self.account_config[combo]["add"][coin] / sum(self.account_config[combo]["add"].values()))
                self.logger.info(f"{account.parameter_name}:{coin},该币可加仓mv为:{coin_ava_mv}")
            if coin_ava_mv > 0:
                coin_price = self.get_coin_price(coin = coin.upper()) if combo.split("_")[1] != "usd" else self.get_usd_contractsize(coin = coin.upper())
                # 已持有币加仓
                if coin in list(now_pos.index):
                    parameter.loc[coin, 'position'] += coin_ava_mv * account.adjEq / coin_price / 100
                    parameter.loc[coin, 'portfolio'] = 1
                    self.logger.info(f"{account.parameter_name}:{coin},已持有币加仓，新加仓位占剩余仓位比为{coin_ava_mv / res_mv * 100}%，新加position为:'{coin_ava_mv * account.adjEq / coin_price / 100}")
                # 新币加仓
                elif len(list(now_pos.index)) >= self.account_config[combo]["coin_amount_uplimit"]:
                    self.logger.info(f"{account.parameter_name}:{coin},'目前持有币数为{len(list(now_pos.index))},大于币数上限{self.account_config[combo]['coin_amount_uplimit']},该币加仓跳过")
                else:
                    parameter.loc[coin, ['position', 'portfolio', 'is_long', 'combo']] = [coin_ava_mv * account.adjEq / coin_price / 100, 1, self.account_config[combo]['is_long'][coin], combo]
                    parameter.loc[coin, ["master_pair", "slave_pair"]] = account.get_pair_name(coin, combo)
                    parameter.loc[coin, ["master_secret", "slave_secret"]] = account.get_secrect_name(coin, combo)
                    self.logger.info(f"{account.parameter_name}:{coin},新币加仓，新加仓位占剩余仓位比为{coin_ava_mv / res_mv * 100}%,新加position为：{coin_ava_mv * account.adjEq / coin_price / 100}")
        return parameter
    
    def check_reduce(self, account: AccountOkex, combo: str) -> bool:
        """检查是否需要reduce减仓
        """
        if self.account_config[combo]['reduce'] == {}:
            self.logger.warning(f"{account.parameter_name}:可选减仓币数为{self.account_config[combo]['reduce']},不执行减仓操作。")
            is_reduce = False
        else:
            self.logger.info(f"{account.parameter_name}:可选减仓币数大于0，执行减仓操作。")
            is_reduce = True
        return is_reduce
    
    def reduce_mv(self, account: AccountOkex, combo: str) -> pd.DataFrame:
        """reduce减仓
        """
        parameter = account.parameter
        reduce = self.account_config[combo]['reduce']
        now_pos = account.position[account.position["combo"] == combo].copy()
        for coin in reduce.keys():
            # 不操作
            if coin not in now_pos.index:
                self.logger.warning(f"{account.parameter_name}:目前没有持有{coin},该币减仓将不会执行")
            elif reduce[coin] >= now_pos.loc[coin, 'MV%']:
                self.logger.info(f"{account.parameter_name}:{coin}的目前仓位为{now_pos.loc[coin]['MV%']}，小于目标仓位{reduce[coin]},该币将不执行减仓操作。")
            # 清仓
            elif reduce[coin] == 0 and now_pos.loc[coin, 'MV%'] != 0:
                parameter.loc[coin, 'portfolio'] = -1
                self.logger.info(f"{account.parameter_name}:{coin}的目标仓位为{reduce[coin]}，该币将减仓至0，同时修改closemaker")
            # 二档减仓
            else:
                target_pos = reduce[coin] / now_pos.loc[coin, 'MV%'] * now_pos.loc[coin, 'position']
                parameter.loc[coin, ['position', 'portfolio']] = [target_pos, -2]
                self.logger.info(f"{account.parameter_name}:{coin}的目标仓位为{reduce[coin]}，该币将二档减仓至目标仓位")
        return parameter
    
    def get_spreads_data(self, combo: str, coin: str, suffix: str = "") -> pd.DataFrame:
        coin = coin.lower()
        if combo in self.spreads.keys() and coin in self.spreads[combo].keys():
            ret = self.spreads[combo][coin]
        else:
            ret = list(self.accounts.values())[0].get_spreads(coin, combo, suffix)
            if combo not in self.spreads.keys():
                self.spreads[combo] = {coin: ret.copy()}
            else:
                self.spreads[combo][coin] = ret.copy()
        return ret
    
    def get_open1(self, account: AccountOkex, coin: str, is_long: bool) -> float:
        open1 = np.nan
        if coin in account.parameter.index:
            combo = account.parameter.loc[coin, "combo"]
            config = self.account_config[combo] if combo in self.account_config.keys() else self.account_config["default"]
            if config["open"] == []:
                spread = self.get_spreads_data(combo, coin, suffix=config["future_date"][0])
                col = "bid0_spread" if is_long else "ask0_spread"
                open1 = self.calc_up_thresh(spread[col], threshold=config['open_thresh'], up_down=0) + config['open_add']
            else:
                open1 = config["open"][0]
        return open1

    def get_closemaker(self, account: AccountOkex, coin: str, is_long: bool) -> float:
        closemaker = np.nan
        if coin in account.parameter.index:
            combo = account.parameter.loc[coin, "combo"]
            config = self.account_config[combo] if combo in self.account_config.keys() else self.account_config["default"]
            spread = self.get_spreads_data(combo, coin, suffix=config["future_date"][0])
            col = "ask0_spread" if is_long else "bid0_spread"
            closemaker = self.calc_up_thresh(spread[col], threshold=config['open_thresh'], up_down=0) + config['close_add']
        return closemaker
    
    def get_closemaker2(self, account: AccountOkex, coin: str, is_long: bool) -> float:
        closemaker2 = np.nan
        if coin in account.parameter.index:
            combo = account.parameter.loc[coin, "combo"]
            config = self.account_config[combo]
            spread = self.get_spreads_data(combo, coin, suffix=config["future_date"][0])
            if config["closemaker2"] == []:
                col = "ask0_spread" if is_long else "bid0_spread"
                closemaker2 = self.calc_up_thresh(spread[col], threshold=config['open_thresh'], up_down=0) + config['close_add'] - config["cm2_chg"]
            else:
                closemaker2 = config['closemaker2'][0]
        return closemaker2
    
    def get_open_close(self, account: AccountOkex, combo: str) -> pd.DataFrame:
        """处理开关仓阈值
        """
        parameter = account.parameter
        config = self.account_config[combo] if combo in self.account_config.keys() else self.account_config["default"]
        maxloss = config["maxloss"]
        for coin in parameter[parameter["combo"] == combo].index:
            level = parameter.loc[coin, "portfolio"]
            parameter.loc[coin, "open"] = max(self.get_open1(account,coin,is_long=parameter.loc[coin, "is_long"]), maxloss) if level == 1 else 2
            parameter.loc[coin, "closemaker"] = max(self.get_closemaker(account,coin,is_long=parameter.loc[coin, "is_long"]), maxloss) if level == -1 else config["closemaker"][0]
            parameter.loc[coin, "closemaker2"] = max(self.get_closemaker2(account,coin,is_long=parameter.loc[coin, "is_long"]), maxloss) if level == -2 else parameter.loc[coin, "closemaker"] - config["cm2_chg"]
            parameter.loc[coin, "open2"] = parameter.loc[coin, "open"] + 1
            parameter.loc[coin, "closetaker"] = parameter.loc[coin, "closemaker"] + 0.001 if config["closetaker"] == [] else config['closetaker'][0]
            parameter.loc[coin, "closetaker2"] = parameter.loc[coin, "closemaker2"] + 0.001 if config["closetaker2"] == [] else config['closetaker2'][0]
            parameter.loc[coin, "position_multiple"] = config["position_multiple"]
        return parameter
    
    def get_position2(self, account: AccountOkex) -> pd.DataFrame:
        for coin in account.parameter.index:
            hold = account.position.loc[coin, "position"] if coin in account.position.index else 0
            account.parameter.loc[coin, "position2"] = 2 * max(hold, account.parameter.loc[coin, "position"])
        return account.parameter
    
    def get_fragment(self, account: AccountOkex) -> pd.DataFrame:
        for coin in account.parameter.index:
            combo = account.parameter.loc[coin, "combo"]
            coin_price = self.get_coin_price(coin = coin.upper()) if combo.split("_")[1] != "usd" else self.get_usd_contractsize(coin = coin.upper())
            account.parameter.loc[coin, ["fragment", "fragment_min"]] = [self.account_config[combo]["fragment_u"] / coin_price, self.account_config[combo]["fragment_min_u"] / coin_price]
        return account.parameter
    
    def handle_future_suffix(self, account: AccountOkex, combo: str) -> pd.DataFrame:
        config = self.account_config[combo] if combo in self.account_config.keys() else self.account_config["default"]
        for coin in account.parameter[account.parameter["combo"] == combo].index:
            account.parameter.loc[coin, "master_pair"] = account.parameter.loc[coin, "master_pair"].replace("future", config["future_date"][0])
            account.parameter.loc[coin, "slave_pair"] = account.parameter.loc[coin, "slave_pair"].replace("future", config["future_date"][0])
            account.parameter.loc[coin, "funding_stop_open"] = config["funding_stop_open"]
            account.parameter.loc[coin, "funding_stop_close"] = config["funding_stop_close"]
            account.parameter.loc[coin, "chase_tick"] = config["chase_tick"]
        return account.parameter
    
    def arrange_parameter(self, account: AccountOkex):
        self.get_position2(account)
        self.get_fragment(account)
        account.parameter["account"] = account.parameter_name
        account.parameter['coin'] = account.parameter["master_pair"]
        account.parameter['timestamp'] = datetime.datetime.utcnow() + datetime.timedelta(hours = 8, minutes= 5)
        account.parameter.set_index('account', inplace=True)
        account.parameter.dropna(how='all', axis=1, inplace=True)
        account.parameter.drop("combo", axis = 1, inplace=True) if "combo" in account.parameter.columns else None
    
    def get_parameter(self, input_accounts: list[str]=[], ingore_account: list[str]=[], is_save=True) -> dict[str, pd.DataFrame]:
        """获取parameter
        """
        account_names = self.get_all_names() - set(ingore_account) if len(input_accounts) == 0 else set(input_accounts) - set(ingore_account)
        all_parameter = {}
        for name in account_names:
            account = self.accounts[name]
            now_pos = self.handle_position(account = account)
            account.parameter = self.init_parameter(now_pos=now_pos, account = account)
            for combo in set(now_pos["combo"].values) | self.combo:
                position = now_pos[now_pos["combo"] == combo].copy()
                if combo in self.account_config.keys() and name in self.account_config[combo]['accounts'].keys():
                    config = self.account_config[combo]
                    if position['MV%'].sum() > config['accounts'][name][0] * config['max_mv_multiple']:
                        account.parameter = self.total_mv_reduce(account, combo)
                    else:
                        self.logger.info(f"{name}:目前仓位为：{position['MV%'].sum()}小于仓位上限{config['accounts'][name][0] * config['max_mv_multiple']}，总仓位没有超限。")
                        account.parameter = self.add_mv(account = account, combo = combo) if self.check_add(account = account, combo = combo) else account.parameter
                        account.parameter = self.reduce_mv(account = account, combo = combo) if self.check_reduce(account = account, combo = combo) else account.parameter
                self.handle_future_suffix(account, combo)
                account.parameter = self.get_open_close(account, combo)
            self.arrange_parameter(account)
            all_parameter[account.parameter_name] = account.parameter
            self.logger.warning(f"{account.parameter_name}:parameter为空！") if len(account.parameter) == 0 else None
        self.parameter = all_parameter.copy()
        if len(all_parameter) > 0 and is_save:
            self.logger.info(f"开始保存到本地！")
            self.save_excel(all_parameter=all_parameter, folder = self.folder,p="/data/buffet2.0/datas")
            self.logger.info(f"parameter保存本地成功！")
        elif len(all_parameter) == 0:
            print('所有parameter表为空')
            self.logger.warning(f"所有账户parameter为空！")
        return all_parameter
    
    def run_buffet(self, is_save = True, upload = False) -> None:
        """main function
        Args:
            is_save (bool, optional): save excel or not. Defaults to True.
            upload (bool, optional): upload excel to github or not. Defaults to False.
        """
        self.print_log()
        self.initilize()
        self.get_parameter(is_save = is_save)
        if upload:
            self.upload_parameter()
        # try:
        #     self.initilize()
        #     self.get_parameter(is_save = is_save)
        # except Exception as e:
        #     self.log_bug(e)
        # if upload:
        #     try:
        #         self.upload_parameter()
        #     except Exception as e:
        #         self.log_bug(e)
        self.logger.handlers.clear()