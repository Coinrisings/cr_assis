import os, datetime, glob, configparser, json, traceback, logging, copy, io
import pandas as pd
import numpy as np
from research.utils.ObjectDataType import AccountData
from research.utils.initAccounts import InitAccounts

git_config = configparser.ConfigParser()
from github import Github
from github.Repository import Repository
from research.utils import readData


class Get_Parameter():
    def __init__(self):
        self.json_path = f"{os.environ['HOME']}/parameters/buffet2.0_config"
        self.log_path = "/data/buffet2.0/datas"
        self.accounts : dict[str, dict[str, AccountData]]
        self.token_path = f"{os.environ['HOME']}/.git-credentials"
        self.parameter: dict[str, dict[str, pd.DataFrame]] = {}
        print('start!')

    def save_excel(self, all_parameter: dict[str, pd.DataFrame], folder: str, p="/data/buffet2.0/datas"):

        today = str(datetime.date.today())
        path = os.environ['HOME'] + p
        base_path_save = f"{path}/parameter/{today}/"

        all_combo = {}
        for acc in all_parameter.keys():
            all_combo[acc] = folder

        result = []
        dic = {}
        for key, value in all_combo.items():
            if not value:
                continue
            if value not in dic.keys():
                dic[value] = []
            dic[value].append(key)

        name = str(datetime.datetime.utcnow())[:19].replace("-", "_")
        name = name.replace(" ", "_").replace(":", "_")
        name = 'buffet2.0_parameter_' + name

        all_file_name = []
        for k in dic.keys():

            path_save = f"{base_path_save}{k}/"
            if not os.path.exists(path_save):
                os.makedirs(path_save)

            file_name = f"{path_save}{name}.xlsx"
            all_file_name.append(file_name)
            excel = pd.ExcelWriter(file_name)
            for i in dic[k]:
                all_parameter[i].to_excel(excel, sheet_name=i, index=True)
            excel.close()
        return all_file_name

    def load_github(self) -> Repository:
        """加载github parameters仓库
        """
        path = self.token_path
        with open(path, "r") as f:
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
    
    def merge_parameter(self) -> dict[str, dict[str, pd.DataFrame]]:
        """合并parameter表
        Returns:
            dict[str, dict[str, pd.DataFrame]]: 一个folder下只有一个excel，同一个账户名的parameter合并到一个sheet里面
        """
        self.merged_parameter : dict[str, dict[str, pd.DataFrame]] = {}
        self.get_folders() if not hasattr(self, "git_folder") else None
        for folder, accounts in self.git_folder.items():
            all_parameter = {}
            for account in accounts:
                if hasattr(account, "parameter") and len(account.parameter) > 0:
                    all_parameter[account.parameter_name] = pd.concat([all_parameter[account.parameter_name], account.parameter]) if account.parameter_name in all_parameter.keys() else account.parameter
            self.merged_parameter[folder] = copy.deepcopy(all_parameter)
        return self.merged_parameter
    
    # 上传参数
    def upload_parameter(self):
        repo = self.load_github()
        self.merge_parameter()
        for folder, parameters in self.merged_parameter.items():
            self.delete_parameter(folder = folder)
            towrite = io.BytesIO()
            writer = pd.ExcelWriter(towrite, engine='openpyxl')
            for sheet_name, parameter in parameters.items():
                parameter["timestamp"] = datetime.datetime.utcnow() + datetime.timedelta(hours = 8, minutes=5)
                parameter.to_excel(excel_writer=writer, sheet_name=sheet_name)
            writer.close()
            upload_time = datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")
            data = towrite.getvalue()
            name = f"excel/{folder}/buffet2.0_parameter_{upload_time}.xlsx"
            repo.create_file(name, f"uploaded by buffet at {upload_time}", data)  # 显示上传字段
            self.logger.info(f"{name} uploaded at {datetime.datetime.utcnow()}")
        return True

    # 计算价差
    def calc_up_thresh(self, spreads, threshold=50, up_down=0):
        spreads_avg = np.mean(spreads)
        spreads_minus_mean = spreads - spreads_avg
        up_amp = spreads_minus_mean.iloc[np.where(spreads_minus_mean > 0)]
        up_thresh = np.percentile(up_amp, [threshold]) + spreads_avg if len(up_amp) > 0 else [np.nan]
        up_thresh = up_thresh[0] + up_down
        return up_thresh

    # 打印保存日志
    def print_log(self):
        p = self.log_path
        Log_Format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        today = str(datetime.date.today())
        path = os.environ['HOME'] + p
        path_save = f"{path}/logs/{today}/"

        if not os.path.exists(path_save):
            os.makedirs(path_save)
        name = str(datetime.datetime.utcnow())[:19].replace("-", "_")
        name = name.replace(" ", "_")
        name = name.replace(":", "_")
        file_name = f"{path_save}{name}.log"
        logger = logging.getLogger(__name__)
        logger.setLevel(level=logging.DEBUG)
        handler = logging.FileHandler(filename=file_name, encoding="UTF-8")
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(Log_Format)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        self.logger = logger
        return logger

    # 获取mr
    def obtain_mr(self, client, username):
        t = pd.to_datetime(datetime.datetime.utcnow()) + pd.Timedelta('8h')
        start = readData.transfer_time(t - pd.Timedelta(600, unit='s'))
        end = readData.transfer_time(t)
        a = f"""SELECT "mr","pair" FROM margin_ratio WHERE client='{client}' AND username='{username}'
        AND ("time">='{start}') AND ("time"<='{end}') group by ex_field,exchange,pair,secret_id,settlement order by time desc limit 1 """
        df = readData.read_influx(a)
        return df

    def load_config(self) -> dict[str, dict]:
        """加载指定路径下的config
        """
        logger = self.logger if hasattr(self, "logger") else self.print_log()
        account_config = {}
        path = self.json_path
        for file_path in glob.glob(f"{path}/*/"):
            lt=os.listdir(file_path)
            for l in lt:
                try:
                    with open(f"{file_path}{l}", "r") as file:
                        file_content = json.load(file)
                        account_config.update(file_content)
                except:
                    logger.warning(f"{file_path+l}该业务线没有config！")
        self.account_config: dict[str, dict] = account_config
        return account_config
    
    def init_combo(self) -> None:
        """找到需要出参的combo，判断条件是json中有并且accounts数量不为0的combo
        """
        self.combo: set[str] = set()
        self.load_config() if not hasattr(self, "account_config") else None
        for combo, data in self.account_config.items():
            self.combo.add(combo) if len(data["accounts"]) > 0 else None
        
    def init_accounts(self) -> dict[str, dict[str, AccountData]]:
        """按照不同combo初始化account
        """
        self.accounts : dict[str, dict[str, AccountData]] = {}
        self.init_combo() if not hasattr(self, "combo") else None
        init = InitAccounts()
        for combo in self.combo:
            init.combo = combo
            init.init_accounts()
            self.accounts[combo] = init.accounts
        return self.accounts

    def get_folders(self):
        """整理所有的github上传路径, 并将上传路径相同的账户归类
        """
        self.folders: set[str] = set()
        self.git_folder : dict[str, set[AccountData]] = {}
        self.init_accounts() if not hasattr(self, "accounts") else None
        for accounts in self.accounts.values():
            for account in accounts.values():
                folder = account.folder
                self.folders.add(folder)
                self.git_folder[folder] = (self.git_folder[folder] | set([account])) if folder in self.git_folder.keys() else set([account])
    
    def initilize(self) -> None:
        """初始化需要出参的combo和accounts, 并且归类路径
        """
        self.load_config()
        self.init_combo()
        self.init_accounts()
        self.get_folders()

    # 获取parameter
    def get_parameter(self, com='binance_busd_swap-binance_usdt_swap', accounts=[], ingore_account=['lxy_003'], is_save = True):

        logger = self.print_log()
        account_config = self.account_config if hasattr(self, "account_config") else self.load_config()
        # account_config = {}
        # path = os.environ['HOME'] + "/parameters/buffet2.0_config"
        # for file_path in glob.glob(f"{path}/*/"):
        #     try:
        #         with open(f"{file_path}buffet2.0_config.json", "r") as file:
        #             file_content = json.load(file)
        #             account_config.update(file_content)
        #     except:
        #          logger.warning(f"{file_path}该业务线没有config！")
                    
        # logger.info(f"获取到的config为:{account_config}！")

        # with open('/home/scq/jupyter/buffet2.0/config/buffet2.0_config.json') as f:
        #     account_config = json.loads(f.read())

        # 获取contract_size 
        p1 = os.environ['HOME'] + '/parameters/config_buffet/dt/contractsize.csv'
        contractsize = pd.read_csv(p1, index_col=0)

        add = account_config[com]['add']
        reduce = account_config[com]['reduce']
        maxloss = account_config[com]['maxloss']
        
        if len(accounts) == 0:
            accounts0 = list(set(account_config[com]['accounts']) & set(self.accounts[com].keys()))
            accounts = list(set(accounts0) - set(ingore_account))
        else:
            accounts = accounts
            accounts = list(set(accounts) - set(ingore_account))

        if len(accounts) == 0:
            all_parameter = {}
            logger.warning(f"{com} 该业务线没有需要出参的账户！")

        else:
            logger.info(f"{com} 该业务线需要出参的账户有：{accounts}！")
            all_parameter = {}

            logger.info(f"{com} 开始遍历该业务线账户，共有账户：{len(accounts)}个！")
            
            
            for acc in accounts:
                exec(f"""{acc} = self.accounts[com]['{acc}']""")
                try:
                    config = account_config[eval(acc).combo]
                except:
                    logger.warning(f"{acc}:获取config错误!")
                    
                # 权益获取，获取不到跳过
                try:
                    eq = eval(acc).get_equity()
                    equity = eval(acc).adjEq
                    
                except:
                    equity=np.nan
                    logger.warning(f"{acc}:获取equity错误!")
                    continue
                    
                if equity is np.nan:
                    logger.warning(f"{acc}:没有获取到当前equity!")
                    continue
                else:
                    pass

                # 仓位获取，获取不到跳过
                try:
                    eval(acc).get_account_position()
                    now_pos = eval(acc).position
                    now_pos.set_index('coin', inplace=True)
                    now_pos = now_pos[now_pos['MV'] > min(config['min_select_u'], equity * config['min_select_ratio'])]
                    coins = list(now_pos.index)
                    logger.info(f"{acc}:当前获取到的position为：{now_pos.to_dict()}")

                except:
                    logger.warning(f"{acc}:没有获取最近10分钟position!")
                    continue

                # mr获取，获取不到跳过
                mr = self.obtain_mr(eval(acc).slave_client, eval(acc).slave_username)

                if len(mr) > 0:
                    logger.info(f"{acc}:当前获取到的mr为：{mr[['pair', 'mr', 'dt']]}")
                else:
                    logger.warning(f"{acc}:没有获取到当前的mr。")
                    continue

                # 初始parameter
                parameter = pd.DataFrame(index=coins,
                                        columns=['account', 'coin', 'portfolio', 'open', 'closemaker', 'position',
                                                'closetaker',
                                                'open2', 'closemaker2', 'position2', 'closetaker2', 'fragment',
                                                'fragment_min', 'funding_stop_open',
                                                'funding_stop_close', 'position_multiple', 'timestamp', 'is_long',
                                                'chase_tick', 'master_pair', 'slave_pair'])

                # 如果账户持有，赋值为原来参数
                if len(now_pos) > 0:

                    logger.info(f"{acc}:非新账户，初始化目前已经持有币的parameter参数。")
                    # print(acc,'老账户,将目前持有的币初始化参数')
                    now_pos['side'] = now_pos[['side']].applymap(lambda x: 1 if x == 'long' else 0)
                    parameter.loc[coins, 'account'] = eval(acc).parameter_name
                    parameter.loc[coins, 'portfolio'] = 0
                    parameter.loc[coins, 'open'] = 2
                    parameter.loc[coins, 'closemaker'] = config['closemaker'][0]
                    parameter.loc[coins, 'position_multiple'] = config['position_multiple']
                    parameter.loc[coins, 'chase_tick'] = config['chase_tick']
                    parameter.loc[coins, 'position'] = now_pos['position']
                    parameter.loc[coins, 'is_long'] = now_pos['side']

                # 新账户，所有参数默认为空
                else:
                    logger.info(f"{acc}:新账户，初始化parameter，参数默认为空。")
                    # print(acc,'新账户，所有参数默认为空') 

                '''
                总MV%超限减仓
                '''
                now_mv = now_pos['MV%'].sum()

                if now_mv > config['accounts'][acc][0] * config['max_mv_multiple']:
                    mv_plus = now_mv - config['accounts'][acc][0] * config['reduce_mv_multiple']
                    logger.info(
                        f"{acc}:总仓位为：{now_mv} 大于 {config['accounts'][acc][0] * config['max_mv_multiple']}，进行总仓位超限减仓，{mv_plus}%的仓位将会被减掉。")
                    if mv_plus > 0:
                        rank = dict(sorted(add.items(), key=lambda x: x[1]))
                        hold_coin = []
                        for i in list(rank.keys()):
                            if i in coins:
                                hold_coin.append(i)

                        if len(hold_coin) == 0:
                            logger.warning(f"{acc}:总仓位超限，但是没有指定排序，所以将不会执行仓位超限减仓！")
                        for c in hold_coin:
                            if mv_plus > 0:
                                ava_reduce = now_pos.loc[c, 'MV%']
                                if ava_reduce <= mv_plus:
                                    parameter.loc[c, 'portfolio'] = -1
                                    spread = eval(acc).get_spreads(c)
                                    if parameter.loc[c, 'is_long'] == 1:
                                        parameter.loc[c, 'closemaker'] = self.calc_up_thresh(spread['ask0_spread'],
                                                                                            threshold=config[
                                                                                                'close_thresh'],
                                                                                            up_down=0) + config[
                                                                            'close_add']
                                    else:
                                        parameter.loc[c, 'closemaker'] = self.calc_up_thresh(spread['bid0_spread'],
                                                                                            threshold=config[
                                                                                                'close_thresh'],
                                                                                            up_down=0) + config[
                                                                            'close_add']
                                    logger.info(f"{acc}:{c}目前mv为{ava_reduce}%，小于{mv_plus}%,该币将会被一档减仓至0")
                                    mv_plus -= ava_reduce
                                else:
                                    parameter.loc[c, 'portfolio'] = -2
                                    parameter.loc[c, 'position'] -= now_pos.loc[c, 'position'] * mv_plus / now_pos.loc[
                                        c, 'MV%']
                                    logger.info(f"{acc}:{c}目前mv为{ava_reduce}%，该币将会被二档减仓至{ava_reduce - mv_plus}%。")
                                    mv_plus -= ava_reduce


                            else:
                                break
                else:
                    logger.info(
                        f"{acc}:目前仓位为：{now_mv}小于仓位上限{config['accounts'][acc][0] * config['max_mv_multiple']}，总仓位没有超限。")

                    '''
                    加仓
                    '''
                    if add == {} or sum(add.values()) == 0:
                        logger.warning(f"{acc}:可选加仓币数为{add},不执行加仓操作。")

                    else:

                        if min(mr['mr'])< config['accounts'][acc][1]:
                            logger.warning(f"{acc}:目前账户有合约对应的mr低于{config['accounts'][acc][1]},不执行加仓操作。")

                        else:
                            # 计算可加仓mv%
                            uplimit_mv = config['accounts'][acc][0]
                            res_mv = max(np.array(0), uplimit_mv - now_mv)
                            res_mv1=res_mv

                            if res_mv <= 0:
                                logger.info(f"{acc}:剩余可加仓mv为：{res_mv}%，不执行加仓操作。")

                            # 如果可以加仓，遍历需要加仓的币
                            else:
                                logger.info(f"{acc}:剩余可加仓mv为：{res_mv}%，执行加仓操作。")

                                for coin in list(add.keys()):

                                    if res_mv <= 0:
                                        break

                                    else:
                                        try:
                                            hold_coin_mv = now_pos.loc[coin, 'MV%']
                                        except:
                                            hold_coin_mv = 0

                                        if hold_coin_mv > config['signal_uplimit_mv'][coin]:
                                            logger.info(f"{acc}:{coin},'该币单币上限超限，该币加仓跳过")
                                            continue
                                        else:
                                            coin_ava_mv = min(config['signal_uplimit_mv'][coin] - hold_coin_mv,
                                                              res_mv1 * add[coin] / sum(list(add.values())))
                                            logger.info(f"{acc}:{coin},'该币可加仓mv为:{coin_ava_mv}")

                                        coin_price = eval(acc).get_coin_price(coin=coin, kind=eval(acc).kind_master)
                                        spread = eval(acc).get_spreads(coin)

                                        if eval(acc).master.split('_')[1] == 'usd' and eval(acc).exchange_master in [
                                            "okx", "okex", "okex5", "ok", "o", "okexv5"]:
                                            master = 'okex-' + eval(acc).master.split('_')[1] + '-' + \
                                                    eval(acc).master.split('_')[2]
                                            coin_price = contractsize.loc[coin.upper(), master]

                                        elif eval(acc).master.split('_')[1] == 'usd' and eval(
                                                acc).exchange_master not in ["okx", "okex", "okex5", "ok", "o",
                                                                            "okexv5"]:
                                            master = eval(acc).master.replace('_', '-')
                                            coin_price = contractsize.loc[coin.upper(), master]


                                        else:
                                            print(acc, 'u本位，币价为实时价格')
                                            pass

                                        # 已持有币加仓
                                        if coin in list(now_pos.index):

                                            if coin_ava_mv * equity / coin_price / 100 > 0:
                                                parameter.loc[
                                                    coin, 'position'] += coin_ava_mv * equity / coin_price / 100
                                                parameter.loc[coin, 'portfolio'] = 1

                                                if config['open'] == []:
                                                    if parameter.loc[coin, 'is_long'] == 1:
                                                        parameter.loc[coin, 'open'] = self.calc_up_thresh(
                                                            spread['bid0_spread'], threshold=config['open_thresh'],
                                                            up_down=0) + config['open_add']
                                                    else:
                                                        parameter.loc[coin, 'open'] = self.calc_up_thresh(
                                                            spread['ask0_spread'], threshold=config['open_thresh'],
                                                            up_down=0) + config['open_add']
                                                    parameter.loc[coin, 'open'] = max(maxloss, parameter.loc[coin, 'open'])
                                                else:
                                                    parameter.loc[coin, 'open'] = config['open'][0]

                                                res_mv -= coin_ava_mv
                                                logger.info(
                                                    f"{acc}:{coin},'已持有币加仓，新加仓位占剩余仓位比为{coin_ava_mv / res_mv1 * 100}%，新加position为:'{coin_ava_mv * equity / coin_price / 100},剩余可加仓mv为：{res_mv}")

                                            else:
                                                logger.info(
                                                    f"{acc}:{coin},'已持有币加仓，新加仓位占剩余仓位比为{coin_ava_mv / res_mv1 * 100}%，新加position为0,,剩余可加仓mv为：{res_mv}")

                                        # 新币加仓
                                        else:
                                            if len(list(now_pos.index)) >= config["coin_amount_uplimit"]:
                                                logger.info(
                                                    f"{acc}:{coin},'目前持有币数为{len(list(now_pos.index))},大于币数上限{config['coin_amount_uplimit']},该币加仓跳过")
                                                continue

                                            else:
                                                if (add[coin] != 0):
                                                    parameter.loc[coin, 'position'] = coin_ava_mv * equity / coin_price / 100
                                                    parameter.loc[coin, 'account'] = acc
                                                    parameter.loc[coin, 'portfolio'] = 1
                                                    parameter.loc[coin, 'closemaker'] = config['closemaker'][0]
                                                    parameter.loc[coin, 'position_multiple'] = config[
                                                        'position_multiple']
                                                    parameter.loc[coin, 'chase_tick'] = config['chase_tick']
                                                    is_long = config['is_long'][coin]
                                                    parameter.loc[coin, 'is_long'] = is_long

                                                    if config['open'] == []:
                                                        if is_long == 1:
                                                            parameter.loc[coin, 'open'] = self.calc_up_thresh(
                                                                spread['bid0_spread'], threshold=config['open_thresh'],
                                                                up_down=0) + config['open_add']
                                                        else:
                                                            parameter.loc[coin, 'open'] = self.calc_up_thresh(
                                                                spread['ask0_spread'], threshold=config['open_thresh'],
                                                                up_down=0) + config['open_add']
                                                            
                                                        parameter.loc[coin, 'open'] = max(maxloss, parameter.loc[coin, 'open'])    
                                                        
                                                    else:
                                                        parameter.loc[coin, 'open'] = config['open'][0]

                                                    res_mv -= coin_ava_mv
                                                    logger.info(
                                                        f"{acc}:{coin},'新币加仓，新加仓位占剩余仓位比为{coin_ava_mv / res_mv1 * 100}%,新加position为：{coin_ava_mv * equity / coin_price / 100},剩余可加仓mv为：{res_mv}")
                                                else:
                                                    logger.info(
                                                        f"{acc}:{coin},'新币加仓，新加仓位占剩余仓位比为{coin_ava_mv / res_mv1 * 100}%,新加position为0,,剩余可加仓mv为：{res_mv}")

                    '''
                    减仓
                    '''
                    if reduce == {}:
                        logger.warning(f"{acc}:可选减仓币数为{reduce},不执行减仓操作。")

                    else:
                        logger.info(f"{acc}:可选减仓币数大于0，执行减仓操作。")
                        for coin in reduce.keys():
                            try:
                                _ = now_pos.loc[coin]['MV%']
                            except:
                                now_pos.loc[coin, 'MV%'] = 0
                                logger.warning(f"{acc}:目前没有持有{coin},该币减仓将不会执行")

                            coin_price = eval(acc).get_coin_price(coin=coin, kind=eval(acc).kind_master)
                            spread = eval(acc).get_spreads(coin)

                            if eval(acc).master.split('_')[1] == 'usd' and eval(acc).exchange_master in ["okx", "okex",
                                                                                                        "okex5", "ok",
                                                                                                        "o", "okexv5"]:
                                master = 'okex-' + eval(acc).master.split('_')[1] + '-' + eval(acc).master.split('_')[2]
                                coin_price = contractsize.loc[coin.upper(), master]

                            elif eval(acc).master.split('_')[1] == 'usd' and eval(acc).exchange_master not in ["okx",
                                                                                                                "okex",
                                                                                                                "okex5",
                                                                                                                "ok",
                                                                                                                "o",
                                                                                                                "okexv5"]:
                                master = eval(acc).master.replace('_', '-')
                                coin_price = contractsize.loc[coin.upper(), master]

                            else:
                                print(acc, 'u本位，币价为实时价格')
                                pass

                            # 如果清仓
                            if reduce[coin] == 0 and now_pos.loc[coin, 'MV%'] != 0:
                                parameter.loc[coin, 'portfolio'] = -1
                                if parameter.loc[coin, 'is_long'] == 1:
                                    parameter.loc[coin, 'closemaker'] = self.calc_up_thresh(spread['ask0_spread'],
                                                                                            threshold=config[
                                                                                                'close_thresh'],
                                                                                            up_down=0) + config[
                                                                            'close_add']
                                else:
                                    parameter.loc[coin, 'closemaker'] = self.calc_up_thresh(spread['bid0_spread'],
                                                                                            threshold=config[
                                                                                                'close_thresh'],
                                                                                            up_down=0) + config[
                                                                            'close_add']
                                    
                                # parameter.loc[coin, 'closemaker'] = max(maxloss, parameter.loc[coin, 'closemaker'])
                                logger.info(f"{acc}:{coin}的目标仓位为{reduce[coin]}，该币将减仓至0，同时修改closemaker")

                            # 二档减仓
                            elif reduce[coin] < now_pos.loc[coin]['MV%']:
                                target_pos = reduce[coin] / now_pos.loc[coin, 'MV%'] * now_pos.loc[coin]['position']
                                parameter.loc[coin, 'position'] = target_pos
                                parameter.loc[coin, 'portfolio'] = -2
                                logger.info(f"{acc}:{coin}的目标仓位为{reduce[coin]}，该币将二档减仓至目标仓位")

                            # 不操作
                            else:
                                logger.info(
                                    f"{acc}:{coin}的目前仓位为{now_pos.loc[coin]['MV%']}，小于目标仓位{reduce[coin]},该币将不执行减仓操作。")

                #    计算fragment,fragment_min，closemaker2,coin

                for c in parameter.index:
                    coin_price = eval(acc).get_coin_price(coin=c, kind=eval(acc).kind_master)
                    spread = eval(acc).get_spreads(c)

                    if eval(acc).master.split('_')[1] == 'usd' and eval(acc).exchange_master in ["okx", "okex", "okex5",
                                                                                                "ok", "o", "okexv5"]:

                        master = 'okex-' + eval(acc).master.split('_')[1] + '-' + eval(acc).master.split('_')[2]
                        coin_price = contractsize.loc[c.upper(), master]

                    elif eval(acc).master.split('_')[1] == 'usd' and eval(acc).exchange_master not in ["okx", "okex",
                                                                                                        "okex5", "ok",
                                                                                                        "o", "okexv5"]:

                        master = eval(acc).master.replace('_', '-')
                        coin_price = contractsize.loc[c.upper(), master]

                    else:
                        print(acc, 'u本位，币价为实时价格')
                        pass

                    parameter.loc[c, 'fragment_min'] = config['fragment_min_u'] / coin_price
                    parameter.loc[c, 'fragment'] = config['fragment_u'] / coin_price

                    if config['closemaker2'] == []:
                        if parameter.loc[c, 'is_long'] == 1:
                            parameter.loc[c, 'closemaker2'] = self.calc_up_thresh(spread['ask0_spread'],
                                                                                    threshold=config['close_thresh'],
                                                                                    up_down=0) - config['cm2_chg'] + \
                                                            config['close_add']
                        else:
                            parameter.loc[c, 'closemaker2'] = self.calc_up_thresh(spread['bid0_spread'],
                                                                                    threshold=config['close_thresh'],
                                                                                    up_down=0) - config['cm2_chg'] + \
                                                            config['close_add']
                            
                        parameter.loc[c, 'closemaker2'] = max(maxloss, parameter.loc[c, 'closemaker2'])
                    
                    else:
                        parameter.loc[c, 'closemaker2'] = config['closemaker2'][0]
                        
                    parameter.loc[c, 'coin'] = c+eval(acc).contract_master.replace('future',config['future_date'][0])

                    parameter.loc[c, 'master_pair'] = parameter.loc[c, 'coin']
                    parameter.loc[c, 'slave_pair'] = c+eval(acc).contract_slave.replace('future',config['future_date'][0])

                    try:
                        parameter.loc[c, 'position2'] = max(2 * parameter.loc[c, 'position'],
                                                            now_pos.loc[c, 'position'])
                    except:
                        logger.info(f"{acc}:{c}的目前仓位没有获取到，该币二档将为一档的2倍！")
                        parameter.loc[c,'position2']=parameter.loc[c,'position']*2
                if config['closetaker'] == []:
                    parameter['closetaker'] = parameter['closemaker'] + 0.001
                else:
                    parameter['closetaker'] = config['closetaker'][0]

                parameter['open2'] = parameter['open'] + 1

                if config['closetaker2'] == []:
                    parameter['closetaker2'] = parameter['closemaker2'] + 0.001
                else:
                    parameter['closetaker2'] = config['closetaker2'][0]

                parameter['funding_stop_open'] = config['funding_stop_open']
                parameter['funding_stop_close'] = config['funding_stop_close']
                parameter['timestamp'] = datetime.datetime.utcnow() + pd.Timedelta('8h') + pd.Timedelta('5m')
                parameter.set_index('account', inplace=True)
                parameter.dropna(how='all', axis=1, inplace=True)
                if len(parameter) > 0:
                    all_parameter[eval(acc).parameter_name] = parameter
                else:
                    logger.warning(f"{acc}:parameter为空！")
                eval(acc).parameter = parameter.copy()

            if len(all_parameter) > 0 and is_save:
                logger.info(f"开始保存到本地！")
                excel_name = self.save_excel(all_parameter=all_parameter, folder = eval(acc).folder,p="/data/buffet2.0/datas")
                logger.info(f"parameter保存本地成功！")
            elif len(all_parameter) == 0:
                print('所有parameter表为空')
                logger.warning(f"所有账户parameter为空！")
        self.parameter[com] = all_parameter.copy()
        logger.handlers.clear()
        return all_parameter

    def log_bug(self, e: Exception):
        self.print_log() if not hasattr(self, "logger") else None
        self.logger.critical(e)
        self.logger.critical(traceback.format_exc())
        self.logger.handlers.clear()
    
    def run_buffet(self, is_save = True, upload = False) -> None:
        """main function
        Args:
            save (bool, optional): save excel or not. Defaults to True.
            upload (bool, optional): upload excel to github or not. Defaults to False.
        """
        try:
            self.initilize()
        except Exception as e:
            self.log_bug(e)
        for combo in self.combo:
            try:
                self.get_parameter(com = combo, is_save = is_save)
            except Exception as e:
                self.log_bug(e)
        if upload:
            try:
                self.upload_parameter()
            except Exception as e:
                self.log_bug(e)