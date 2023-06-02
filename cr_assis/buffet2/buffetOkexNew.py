from cr_assis.connect.connectData import ConnectData
from cr_assis.account.accountOkex import AccountOkex
from cr_assis.account.initAccounts import InitAccounts
from github import Github
from github.Repository import Repository
import numpy as np
import pandas as pd
import os, datetime, logging, traceback, io, glob, json, copy, ccxt

class BuffetOkexNew(object):
    
    def __init__(self) -> None:
        self.folder = "pt"
        self.database = ConnectData()
        self.markets = ccxt.okex().load_markets()
        self.json_path = f"{os.environ['HOME']}/parameters/buffet2_config/pt"
        self.save_path = f"{os.environ['HOME']}/data/buffet2.0"
        self.accounts : dict[str, AccountOkex]
        self.is_long = {"long": 1, "short": 0}
        self.exchange_position = "okexv5"
        self.exchange_save = "okex"
        self.now_position: dict[str, pd.DataFrame] = {}
        self.add :dict = {}
        self.token_path = f"{os.environ['HOME']}/.git-credentials"
        self.parameter: dict[str, pd.DataFrame] = {}
        self.coin_price: dict[str, float] = {}
        self.usd_contractsize: dict[str, float] = {}
        self.spreads: dict[str, dict[str, pd.DataFrame]] = {}
        self.contractsize_path: str = os.environ['HOME'] + '/parameters/config_buffet/dt/contractsize.csv'
        self.parameter_cols = ['account', 'coin', 'portfolio', 'open', 'closemaker', 'position', 'closetaker', 'open2', 'closemaker2', 'position2', 'closetaker2', 'fragment',
                            'fragment_min', 'funding_stop_open', 'funding_stop_close', 'position_multiple', 'timestamp', 'is_long', 'chase_tick', 'master_pair', 'slave_pair', "master_secret", "slave_secret", "combo"]
        self.config_keys = set(["default_path", "total_mv", "single_mv", "thresh"])
        self.default_keys = set(['combo', 'funding_open', 'funding_close', 'chase_tick', 'close', 'open', 'closemaker', 'closetaker', 'closemaker2', 'closetaker2', 'cm2_change', 'fragment', 'fragment_min', 'open_add', 'close_add', 'select_u', 'select_ratio', 'maxloss', 'open_thresh', 'close_thresh', 'future_date'])
        self.execute_account: AccountOkex
        self.load_logger()
    
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
    
    def check_config(self, file: str) -> dict:
        try:
            with open(file, "r") as f:
                data = json.load(f)
            ret = self.load_config_default(data) if set(data.keys()) == self.config_keys and len(data["total_mv"]) > 0 else {}
        except:
            ret = {}
            self.logger.warning(f"{file} 加载出错")
        return ret
    
    def connect_account_config(self, config: dict) -> dict:
        ret = {}
        for name in config["total_mv"].keys():
            ret[name] = config
        return ret
    
    def load_config_default(self, config: dict) -> None:
        try:
            path = os.environ["HOME"]+config["default_path"]
            with open(path, "r") as f:
                data = json.load(f)
            config.update(data)
            ret = self.connect_account_config(config) if set(data.keys()) == self.default_keys else {}
        except:
            ret = {}
            self.logger.warning(f"{config}的default加载错误")
        return ret
    
    def load_config(self) -> None:
        self.config: dict[str, dict] = {}
        files = glob.glob(f"{self.json_path}/*.json")
        for file in files:
            self.config.update(self.check_config(file))
            
    
    def load_logger(self) -> None:
        Log_Format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        path_save = f"{self.save_path}/logs/{datetime.date.today()}/"
        os.makedirs(path_save) if not os.path.exists(path_save) else None
        name = datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")
        file_name = f"{path_save}{name}.log"
        logger = logging.getLogger(__name__)
        logger.setLevel(level=logging.DEBUG)
        handler = logging.FileHandler(filename=file_name, encoding="UTF-8")
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(Log_Format)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        self.logger = logger
    
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
    
    def init_accounts(self) -> dict[str, AccountOkex]:
        """初始化所有orch打开的账户 删除获取不到position、adjEq和mr的账户
        """
        self.load_config()
        init = InitAccounts(ignore_test=False)
        self.accounts = init.init_accounts_okex()
        names = set(self.accounts.keys())
        for name in names:
            self.accounts.pop(name, None) if name not in self.config.keys() or self.check_account(account = self.accounts[name]) else None
        return self.accounts
    
    def init_parameter(self) -> pd.DataFrame:
        """初始化parameter
        """
        account = self.execute_account
        now_pos = account.position[(account.position["MV"] > self.config[account.parameter_name]["select_u"]) & (account.position["MV%"] > self.config[account.parameter_name]["select_ratio"])].copy().set_index("coin")
        self.now_position[account.parameter_name] = now_pos.copy()
        parameter = pd.DataFrame(columns=self.parameter_cols, index = now_pos.index)
        if len(now_pos) > 0:
            self.logger.info(f"{account.parameter_name}:非新账户, 初始化目前已经持有币的parameter参数")
            parameter['portfolio'] = 0
            cols = ["position", "master_pair", "slave_pair", "master_secret", "slave_secret", "combo"]
            parameter[cols] = now_pos[cols]
            parameter['is_long'] = now_pos['side'].apply(lambda x: 1 if x == 'long' else 0)
        else:
            self.logger.info(f"{account.parameter_name}:新账户, 初始化parameter, 参数默认为空")
        account.parameter = parameter
        
    def check_single_mv(self, coin: str, values: list) -> bool:
        is_error = True
        if len(values) != 2:
            self.logger.info(f"{self.execute_account.parameter_name}的single_mv填写错误, {coin}的value{values}长度不等于2, 无法进行单币种超限减仓和加仓")
        elif values[1] < 0:
            self.logger.info(f"{self.execute_account.parameter_name}的single_mv填写错误, {coin}的value{values}第二个数字小于0, 无法进行单币种超限减仓和加仓")
        elif abs(values[1]) < abs(values[0]):
            self.logger.info(f"{self.execute_account.parameter_name}的single_mv填写错误, {coin}的value{values}第二个数字绝对值小于第一个, 无法进行单币种超限减仓和加仓")
        else:
            is_error = False
        return is_error

    def execute_reduce(self, coin: str, to_mv: float) -> None:
        if coin not in self.execute_account.parameter.index or to_mv < 0:
            self.logger.info(f"execute_reduce失败, {coin} 不在{self.execute_account.parameter_name}持仓中或者{to_mv}小于0")
            return
        if to_mv > 0:
            self.execute_account.parameter.loc[coin, "position"] = self.now_position[self.execute_account.parameter_name].loc[coin, "position"] * to_mv / self.now_position[self.execute_account.parameter_name].loc[coin, "MV%"]
            self.execute_account.parameter.loc[coin, "portfolio"] = -2
            self.logger.info(f"execute_reduce成功, {self.execute_account.parameter_name}持仓中{coin}将二档减仓至{to_mv}")
        else:
            self.execute_account.parameter.loc[coin, "position"] = self.now_position[self.execute_account.parameter_name].loc[coin, "position"]
            self.execute_account.parameter.loc[coin, "portfolio"] = -1
            self.logger.info(f"execute_reduce成功, {self.execute_account.parameter_name}持仓中{coin}将一档减仓")
        self.now_position[self.execute_account.parameter_name].loc[coin, "position"] *= to_mv / self.now_position[self.execute_account.parameter_name].loc[coin, "MV%"]
        self.now_position[self.execute_account.parameter_name].loc[coin, "MV%"] = to_mv
    
    def execute_add(self, coin: str, to_mv: float, combo: str):
        parameter = self.execute_account.parameter
        if coin in self.now_position[self.execute_account.parameter_name].index and abs(to_mv) > self.now_position[self.execute_account.parameter_name].loc[coin, "MV%"]:
            parameter.loc[coin, "position"] *= max(abs(to_mv) / self.now_position[self.execute_account.parameter_name].loc[coin, "MV%"], 1)
            parameter.loc[coin, "portfolio"] = 1
            self.logger.info(f"{self.execute_account.parameter_name}已持有币种加仓{coin}@{combo}至{to_mv}")
        else:
            price = self.get_usd_contractsize(coin) if combo.split("-")[0].split("_")[1] == "usd" else self.get_coin_price(coin)
            parameter.loc[coin, "position"] = abs(to_mv) / 100 * self.execute_account.adjEq / price
            parameter.loc[coin, "combo"] = combo
            parameter.loc[coin, "portfolio"] = 1
            parameter.loc[coin, "is_long"] = 1 if to_mv > 0 else 0
            parameter.loc[coin, ["master_pair", "slave_pair"]] = self.execute_account.get_pair_name(coin, combo)
            parameter.loc[coin, ["master_secret", "slave_secret"]] = self.execute_account.get_secret_name(coin, combo)
            self.logger.info(f"{self.execute_account.parameter_name}新币种加仓{coin}@{combo}至{to_mv}")
    
    def reduce_single_mv(self):
        now_position = self.now_position[self.execute_account.parameter_name]
        config = self.config[self.execute_account.parameter_name]
        for name, reduce in config["single_mv"].items():
            combo = config["combo"][name] if name in config["combo"].keys() else name
            for coin, values in reduce.items():
                if self.check_single_mv(coin, values):
                    continue
                if coin in now_position.index and now_position.loc[coin, "combo"] == combo and now_position.loc[coin, "MV%"] > values[1]:
                    self.execute_reduce(coin, values[1])
                    self.logger.info(f"{self.execute_account.parameter_name}中{coin}@{combo}触发单币种超限减仓, 减仓至{values[1]}")
                
    def reduce_total_mv(self):
        now_position = self.now_position[self.execute_account.parameter_name]
        config = self.config[self.execute_account.parameter_name]["total_mv"]
        total_mv = now_position["MV%"].sum()
        plus = total_mv - config[self.execute_account.parameter_name][2]
        if plus > 0:
            remove_mv = config[self.execute_account.parameter_name][1]
            for coin in now_position.index:
                to_mv = remove_mv / total_mv
                self.execute_reduce(coin = coin, to_mv = to_mv)
                self.logger.info(f"{self.execute_account.parameter_name}目前总仓位{total_mv}触发单币种超限减仓, 将减仓至{remove_mv}")

    def get_add(self) -> dict[str, dict[str, float]]:
        real_add = {}
        now_position = self.now_position[self.execute_account.parameter_name]
        config = self.config[self.execute_account.parameter_name]
        for name, add in config["single_mv"].items():
            combo = config["combo"][name] if name in config["combo"].keys() else name
            for coin, values in add.items():
                if self.check_single_mv(coin, values) or coin in real_add.keys() or values[0] == 0:
                    continue
                if coin not in self.execute_account.parameter.index and coin not in now_position.index:
                    real_add.update({f"{coin}@{combo}": values[0]})
                elif coin in now_position.index and now_position.loc[coin, "combo"] == combo and now_position.loc[coin, "MV%"] < abs(values[0]) and (now_position.loc[coin, "side"] == "long" and values[0] > 0 or now_position.loc[coin, "side"] == "short" and values[0] < 0):
                    real_add.update({f"{coin}@{combo}": values[0]})
        self.add[self.execute_account.parameter_name] = copy.deepcopy(real_add)
        return real_add
    
    def add_mv(self):
        now_position = self.now_position[self.execute_account.parameter_name]
        config = self.config[self.execute_account.parameter_name]
        ava = config["total_mv"][self.execute_account.parameter_name][0] - now_position["MV%"].sum()
        real_add = self.get_add()
        add_sum = pd.DataFrame.from_dict(real_add, orient='index').abs().values.sum()
        wish_add, holding_mv  = 0, 0
        for k in real_add.keys():
            coin = k.split("@")[0]
            hold = now_position.loc[coin, "MV%"] if coin in now_position.index else 0
            wish_add += abs(real_add[k]) - hold
            holding_mv += hold
        add_to = holding_mv + min(wish_add, ava)
        for k, wish_mv in real_add.items():
            coin, combo = k.split("@")
            to_mv = - min(abs(wish_mv) / add_sum * add_to, wish_add, ava) if wish_mv < 0 else min(abs(wish_mv) / add_sum * add_to, wish_add, ava)
            self.execute_add(coin, to_mv, combo)
    
    def calc_up_thresh(self, spreads, threshold=50, up_down=0):
        spreads_avg = np.mean(spreads)
        spreads_minus_mean = spreads - spreads_avg
        up_amp = spreads_minus_mean.iloc[np.where(spreads_minus_mean > 0)]
        up_thresh = np.percentile(up_amp, [threshold]) + spreads_avg if len(up_amp) > 0 else [np.nan]
        up_thresh = up_thresh[0] + up_down
        return up_thresh
    
    def get_spreads_data(self, combo: str, coin: str, suffix: str = "") -> pd.DataFrame:
        coin = coin.lower()
        if combo in self.spreads.keys() and coin in self.spreads[combo].keys():
            ret = self.spreads[combo][coin]
        else:
            ret = self.execute_account.get_spreads(coin, combo, suffix)
            if combo not in self.spreads.keys():
                self.spreads[combo] = {coin: ret.copy()}
            else:
                self.spreads[combo][coin] = ret.copy()
        return ret
    
    def get_real_thresh(self, combo: str, thresh: str):
        config = self.config[self.execute_account.parameter_name]
        ret = config[thresh] if thresh in config.keys() else np.nan
        for name in config["thresh"].keys():
            if (name == combo or (name in config["combo"].keys() and config["combo"][name] == combo)) and thresh in config["thresh"][name].keys():
                ret = config["thresh"][name][thresh]
                break
        return ret
    
    def get_expect_thresh(self, coin: str, col: str, spread_name: str, use_thresh: str) -> float:
        ret = np.nan
        account = self.execute_account
        if coin in account.parameter.index:
            combo = self.execute_account.parameter.loc[coin, "combo"]
            specify_thresh = self.get_real_thresh(combo, thresh = col)
            if specify_thresh == "":
                maxloss = float(self.get_real_thresh(combo, thresh="maxloss"))
                spread = self.get_spreads_data(combo, coin, suffix=self.get_real_thresh(combo, thresh = "future_date"))
                ret = max(self.calc_up_thresh(spread[spread_name], threshold=float(self.get_real_thresh(combo, thresh = use_thresh)), up_down=0) + float(self.get_real_thresh(combo, thresh = "open_add")), maxloss)
            else:
                ret = float(specify_thresh)
        return ret
    
    def get_open1(self, coin: str) -> float:
        spread_name = "bid0_spread" if self.execute_account.parameter.loc[coin, "is_long"] else "ask0_spread"
        return self.get_expect_thresh(coin, "open", spread_name, use_thresh="open_thresh")

    def get_closemaker(self, coin: str) -> float:
        spread_name = "ask0_spread" if self.execute_account.parameter.loc[coin, "is_long"] else "bid0_spread"
        return self.get_expect_thresh(coin, "open", spread_name, use_thresh="close_thresh")
    
    def get_closemaker2(self, coin: str) -> float:
        spread_name = "ask0_spread" if self.execute_account.parameter.loc[coin, "is_long"] else "bid0_spread"
        combo = self.execute_account.parameter.loc[coin, "combo"] if coin in self.execute_account.parameter.index else ""
        return self.get_expect_thresh(coin, "open", spread_name, use_thresh="close_thresh") + float(self.get_real_thresh(combo, thresh = "cm2_change"))
    
    def get_open_close(self) -> pd.DataFrame:
        """处理开关仓阈值
        """
        parameter = self.execute_account.parameter
        for coin in parameter.index:
            level = parameter.loc[coin, "portfolio"]
            combo = parameter.loc[coin, "combo"]
            parameter.loc[coin, "open"] = self.get_open1(coin) if level == 1 else 2
            parameter.loc[coin, "closemaker"] = self.get_closemaker(coin) if level == -1 else float(self.get_real_thresh(combo, thresh="close"))
            parameter.loc[coin, "closemaker2"] = self.get_closemaker2(coin) if level == -2 or self.get_real_thresh(combo, thresh="closemaker2") != "" else parameter.loc[coin, "closemaker"] + float(self.get_real_thresh(combo, thresh="cm2_change"))
            parameter.loc[coin, "open2"] = parameter.loc[coin, "open"] + 1
            parameter.loc[coin, "closetaker"] = parameter.loc[coin, "closemaker"] + 0.001 if self.get_real_thresh(combo, thresh="closetaker") == "" else float(self.get_real_thresh(combo, thresh="closetaker") == "")
            parameter.loc[coin, "closetaker2"] = parameter.loc[coin, "closemaker2"] + 0.001 if self.get_real_thresh(combo, thresh="closetaker2") == "" else float(self.get_real_thresh(combo, thresh="closetaker2") == "")
        self.arrange_parameter()
        return parameter
    
    def get_position2(self) -> pd.DataFrame:
        account = self.execute_account
        for coin in account.parameter.index:
            hold = account.position.loc[coin, "position"] if coin in account.position.index else 0
            account.parameter.loc[coin, "position2"] = 2 * max(hold, account.parameter.loc[coin, "position"])
    
    def get_fragment(self) -> pd.DataFrame:
        account = self.execute_account
        for coin in account.parameter.index:
            combo = account.parameter.loc[coin, "combo"]
            coin_price = self.get_coin_price(coin = coin.upper()) if combo.split("_")[1] != "usd" else self.get_usd_contractsize(coin = coin.upper())
            account.parameter.loc[coin, ["fragment", "fragment_min"]] = [float(self.get_real_thresh(combo, thresh="fragment")) / coin_price, float(self.get_real_thresh(combo, thresh="fragment_min")) / coin_price]
    
    def handle_future_suffix(self) -> pd.DataFrame:
        account = self.execute_account
        for coin in account.parameter.index:
            combo = account.parameter.loc[coin, "combo"]
            account.parameter.loc[coin, "master_pair"] = account.parameter.loc[coin, "master_pair"].replace("future", self.get_real_thresh(combo, thresh="future_date"))
            account.parameter.loc[coin, "slave_pair"] = account.parameter.loc[coin, "slave_pair"].replace("future", self.get_real_thresh(combo, thresh="future_date"))
            account.parameter.loc[coin, "funding_stop_open"] =  float(self.get_real_thresh(combo, thresh="funding_open"))
            account.parameter.loc[coin, "funding_stop_close"] = float(self.get_real_thresh(combo, thresh="funding_close"))
            account.parameter.loc[coin, "chase_tick"] = float(self.get_real_thresh(combo, thresh="chase_tick"))
        account.parameter["account"] = account.parameter_name
        account.parameter["position_multiple"] = 1
        account.parameter.set_index('account', inplace=True)
        account.parameter['coin'] = account.parameter["master_pair"]
        account.parameter['timestamp'] = datetime.datetime.utcnow() + datetime.timedelta(hours = 8, minutes= 3)
        account.parameter.dropna(how='all', axis=1, inplace=True)
        account.parameter.drop("combo", axis = 1, inplace=True) if "combo" in account.parameter.columns else None
    
    def arrange_parameter(self):
        self.get_position2()
        self.get_fragment()
        self.handle_future_suffix()
        
    def save_parameter(self): 
        path_save = f"{self.save_path}/parameter/{datetime.date.today()}/{self.exchange_save}"
        os.makedirs(path_save) if not os.path.exists(path_save) else None
        file_name = f'{path_save}/buffet2.0_parameter_{datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")}.xlsx'
        excel = pd.ExcelWriter(file_name)
        for name, parameter in self.parameter.items():
            parameter.to_excel(excel, sheet_name=name, index=True)
        excel.close()
    
    def check_total_mv(self) -> bool:
        is_error = True
        config = self.config[self.execute_account.parameter_name]["total_mv"][self.execute_account.parameter_name]
        if len(config) != 3:
            self.logger.warning(f"{self.execute_account.parameter_name}的total_mv填写错误, 长度不等于3")
        elif config[0] < 0 or config[1] < 0 or config[2] < 0:
            self.logger.warning(f"{self.execute_account.parameter_name}的total_mv填写错误, {config}中有小于0的数字")
        elif config[0] > config[1] or config[1] > config[2]:
            self.logger.warning(f"{self.execute_account.parameter_name}的total_mv填写错误, {config}中第一个数字大于第二个或者第二个数字大于第三个")
        else:
            is_error = False
        return is_error
    
    def get_parameter(self) -> None:
        """获得config里面写的并且orch打开的okex账户的parameter
        Args:
            is_save (bool, optional): save parameter. Defaults to True.
        """
        for name, account in self.accounts.items():
            self.execute_account = account
            self.init_parameter()
            self.reduce_single_mv()
            if self.check_total_mv():
                self.logger.warning(f"{name}config中total_mv有误, 不进行总仓位减仓或加仓")
            else:
                self.add_mv() if self.now_position[account.parameter_name]["MV%"].sum() < self.config[account.parameter_name]["total_mv"][account.parameter_name][0] else self.reduce_total_mv()
            self.get_open_close()
            self.parameter[name] = account.parameter
    
    def load_github(self) -> Repository:
        """加载github parameters仓库
        """
        with open(self.token_path, "r") as f:
            config = f.read()
        access_token = config.split(":")[-1].split("@")[0]
        g = Github(login_or_token=access_token)
        repo = g.get_repo("Coinrisings/parameters")
        self.repo = repo
        return repo
    
    def delete_parameter(self, folder: str) -> None:
        """删除原有文件
        """
        self.load_github() if not hasattr(self, "repo") else None
        repo = self.repo
        contents = repo.get_contents(f"excel/{folder}")
        if len(contents) >= 5:
            name = 'buffet2.0_parameter_' + str(datetime.datetime.utcnow() - pd.Timedelta('30m'))[:19].replace("-", "_").replace(
                " ", "_").replace(":", "_")
            for content_file in contents:
                if 'buffet2.0_parameter' in content_file.name and name > content_file.name:
                    repo.delete_file(content_file.path,
                                    message=f"buffet removes {content_file.name} at {datetime.datetime.utcnow()}",
                                    sha=content_file.sha)
                    self.logger.info(f"buffet removes {content_file.name} at {datetime.datetime.utcnow()}")
        else:
            pass
    
    def upload_parameter(self):
        repo = self.load_github()
        self.delete_parameter(folder = self.folder)
        towrite = io.BytesIO()
        writer = pd.ExcelWriter(towrite, engine='openpyxl')
        for sheet_name, parameter in self.parameter.items():
            parameter["timestamp"] = datetime.datetime.utcnow() + datetime.timedelta(hours = 8, minutes=3)
            parameter.to_excel(excel_writer=writer, sheet_name=sheet_name)
        writer.close()
        upload_time = datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")
        data = towrite.getvalue()
        name = f"excel/{self.folder}/buffet2.0_parameter_{upload_time}.xlsx"
        repo.create_file(name, f"uploaded by buffet at {upload_time}", data)  # 显示上传字段
        self.logger.info(f"{name} uploaded at {datetime.datetime.utcnow()}")
    
    def log_bug(self, e: Exception):
        self.logger.critical(e)
        self.logger.critical(traceback.format_exc())
        self.logger.handlers.clear()
    
    def run_buffet(self, is_save = True, upload = False) -> None:
        """main function
        Args:
            is_save (bool, optional): save excel or not. Defaults to True.
            upload (bool, optional): upload excel to github or not. Defaults to False.
        """
        self.init_accounts()
        self.get_parameter()
        if is_save and len(self.parameter):
            self.save_parameter()
        if upload:
            self.upload_parameter()
        # try:
        #     self.init_accounts()
        #     self.get_parameter()
        # except Exception as e:
        #     self.log_bug(e)
        # if is_save:
        #     try:
        #         self.save_parameter()
        #     except Exception as e:
        #         self.log_bug(e)
        # if upload:
        #     try:
        #         self.upload_parameter()
        #     except Exception as e:
        #         self.log_bug(e)
        self.logger.handlers.clear()