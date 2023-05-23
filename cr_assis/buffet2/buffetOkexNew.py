from cr_assis.connect.connectData import ConnectData
from cr_assis.account.accountOkex import AccountOkex
from cr_assis.account.initAccounts import InitAccounts
from github import Github
from github.Repository import Repository
import numpy as np
import pandas as pd
import os, datetime, logging, traceback, io, glob, json

class BuffetOkexNew(object):
    
    def __init__(self) -> None:
        self.folder = "pt"
        self.database = ConnectData()
        self.json_path = f"{os.environ['HOME']}/parameters/buffet2_config/pt"
        self.save_path = f"{os.environ['HOME']}/data/buffet2.0"
        self.accounts : dict[str, AccountOkex]
        self.now_position: dict[str, pd.DataFrame]
        self.token_path = f"{os.environ['HOME']}/.git-credentials"
        self.parameter: dict[str, dict[str, pd.DataFrame]] = {}
        self.coin_price: dict[str, float] = {}
        self.usd_contractsize: dict[str, float] = {}
        self.spreads: dict[str, dict[str, pd.DataFrame]] = {}
        self.contractsize_path: str = os.environ['HOME'] + '/parameters/config_buffet/dt/contractsize.csv'
        self.parameter_cols = ['account', 'coin', 'portfolio', 'open', 'closemaker', 'position', 'closetaker', 'open2', 'closemaker2', 'position2', 'closetaker2', 'fragment',
                            'fragment_min', 'funding_stop_open', 'funding_stop_close', 'position_multiple', 'timestamp', 'is_long', 'chase_tick', 'master_pair', 'slave_pair', "master_secret", "slave_secret", "combo"]
        self.config_keys = set(["default_path", "total_mv", "single_mv", "thresh"])
        self.default_keys = set(['combo', 'funding_open', 'funding_close', 'chase_tick', 'close', 'open', 'closemaker', 'closetaker', 'closemaker2', 'closertaker2', 'cm2_change', 'fragment', 'fragment_min', 'open_add', 'close_add', 'select_u', 'select_ratio', 'maxloss', 'open_thresh', 'close_thresh', 'future_date'])
        self.execute_account: AccountOkex
        self.load_logger()
    
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
            path = os.path.dirname(self.json_path)+config["default_path"]
            with open(path, "r") as f:
                data = json.load(f)
            ret = self.connect_account_config(config.update(data)) if set(data.keys()) == self.default_keys else {}
        except:
            ret = {}
            self.logger.warning(f"{config}的default加载错误")
        return ret
    
    def load_config(self) -> None:
        self.config = {}
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
        if len(values) < 2:
            self.logger.info(f"{self.execute_account.parameter_name}的single_mv填写错误, {coin}的value{values}长度小于2, 无法进行单币种超限减仓")
        elif values[1] < 0:
            self.logger.info(f"{self.execute_account.parameter_name}的single_mv填写错误, {coin}的value{values}第二个数字小于0, 无法进行单币种超限减仓")
        else:
            is_error = False
        return is_error

    def reduce_single_mv(self):
        now_position = self.now_position[self.execute_account.parameter_name]
        config = self.config[self.execute_account.parameter_name]
        for name, reduce in self.config[self.execute_account.parameter_name]["single_mv"]:
            combo = config["combo"][name] if name in config["combo"].keys() else name
            position = now_position[now_position["combo"] == combo].copy()
            for coin, values in reduce.items():
                if self.check_single_mv(coin, values):
                    continue
                if coin in now_position.index and now_position.loc[coin, "combo"] == combo and now_position.loc[coin, "MV%"] > values[1]:
                    now_position.loc[coin, "position"] *= now_position.loc[coin, "MV%"] / values[1]
                    now_position.loc[coin, "MV%"] = values[1]
                    self.execute_account.parameter.loc[coin, "position"] = now_position.loc[coin, "position"]
                    self.execute_account.parameter.loc[coin, "portfolio"] = -2
                
    def reduce_total_mv(self):
        pass
    
    def add_mv(self):
        pass
    
    def get_thresh(self):
        pass
    
    def save_parameter(self): 
        pass
    
    def get_parameter(self, is_save = True) -> None:
        """获得config里面写的并且orch打开的okex账户的parameter
        Args:
            is_save (bool, optional): save parameter. Defaults to True.
        """
        for name, account in self.accounts.items():
            self.execute_account = account
            self.init_parameter()
            self.reduce_single_mv()
            self.reduce_total_mv()
            self.add_mv()
            self.get_thresh()
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
            parameter["timestamp"] = datetime.datetime.utcnow() + datetime.timedelta(hours = 8, minutes=5)
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
        try:
            self.init_accounts()
            self.get_parameter(is_save = is_save)
        except Exception as e:
            self.log_bug(e)
        if upload:
            try:
                self.upload_parameter()
            except Exception as e:
                self.log_bug(e)
        self.logger.handlers.clear()