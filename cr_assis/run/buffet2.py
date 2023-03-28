import sys, os, datetime, glob, time, math, configparser, json
import research
from pathlib import Path
from pymongo import MongoClient
from bson.objectid import ObjectId
import pandas as pd
import numpy as np
from research.eva import eva
import redis
from influxdb import InfluxDBClient
import traceback, logging

git_config = configparser.ConfigParser()
from github import Github
import json
import time
from research.utils import readData


class Get_Parameter():
    def __init__(self, accounts):
        print('start!')
        self.accounts = accounts

    def save_excel(self, all_parameter, p="/jupyter/data/buffet/buffet2_result"):

        today = str(datetime.date.today())
        path = os.environ['HOME'] + p
        base_path_save = f"{path}/parameter/{today}/"

        all_combo = {}
        for account in self.accounts:
            if account.parameter_name in all_parameter.keys():
                all_combo[account.parameter_name] = account.folder
            

        result = []
        dic = {}
        for key, value in all_combo.items():
            if not value:
                continue
            if value not in dic.keys():
                dic[value] = []
            dic[value].append(key)

        name = str(datetime.datetime.now())[:19].replace("-", "_")
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

    # 上传参数
    def upload_parameter(self, excel_name, path=r'/.git-credentials'):

        # 获得github仓库
        path = os.environ['HOME'] + path
        with open(path, "r") as f:
            config = f.read()
        access_token = config.split(":")[-1].split("@")[0]
        g = Github(login_or_token=access_token)
        repo = g.get_repo("Coinrisings/parameters")

        for e in excel_name:
            # 删除原有文件
            contents = repo.get_contents("excel/" + e.split('/')[-2])
            if len(contents) >= 5:
                # name='buffet2.0_parameter_'+str(datetime.datetime.now())[:10].replace("-", "_")
                name = 'buffet2.0_parameter_' + str(datetime.datetime.now() - pd.Timedelta('30m'))[:19].replace("-",
                                                                                                                "_").replace(
                    " ", "_").replace(":", "_")
                for content_file in contents:
                    if 'buffet2.0_parameter' in content_file.name and name > content_file.name:
                        repo.delete_file(content_file.path,
                                         message=f"buffet removes {content_file.name} at {datetime.datetime.now()}",
                                         sha=content_file.sha)
                        print(f"buffet removes {content_file.name} at {datetime.datetime.now()}")
            else:
                pass

            # 读取parameter
            with open(e, "rb") as f:
                data = f.read()
                name = "excel/" + e.split('/')[-2] + '/' + e.split('/')[-1]
                repo.create_file(name, f"uploaded by ssh at {datetime.datetime.now()}", data)  # 显示上传字段
                print(f"{name} uploaded")
                print(datetime.datetime.now())
        return True

    # 计算价差
    def calc_up_thresh(self, spreads, threshold=50, up_down=0):
        spreads_avg = np.mean(spreads)
        spreads_minus_mean = spreads - spreads_avg
        up_amp = spreads_minus_mean.iloc[np.where(spreads_minus_mean > 0)]
        up_thresh = np.percentile(up_amp, [threshold]) + spreads_avg
        up_thresh = up_thresh[0] + up_down
        return up_thresh

    # 打印保存日志
    def print_log(self, p="/data/buffet2.0/datas"):
        Log_Format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        today = str(datetime.date.today())
        path = os.environ['HOME'] + p
        path_save = f"{path}/logs/{today}/"

        if not os.path.exists(path_save):
            os.makedirs(path_save)
        name = str(datetime.datetime.now())[:19].replace("-", "_")
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

    # 获取parameter
    def get_parameter(self, com='binance_busd_swap-binance_usdt_swap', accounts=[], ingore_account=['lxy_003'],
                      upload=False):

        logger = self.print_log(p="/jupyter/data/buffet/buffet2_result")

       # 获取config
    
#         account_config = {}
#         path = os.environ['HOME'] + "/parameters/buffet2.0_config"
#         for file_path in glob.glob(f"{path}/*/"):
#             try:
#                 with open(f"{file_path}buffet2.0_config.json", "r") as file:
#                     file_content = json.load(file)
#                     account_config.update(file_content)
#             except:
#                  logger.warning(f"{file_path}该业务线没有config！")
                    
#         logger.info(f"获取到的config为:{account_config}！")

        with open(f'{str(Path( __file__ ).parent.parent.absolute())}/config/buffet2.0_config.json') as f:
            account_config = json.loads(f.read())

        # 获取contract_size 
        p1 = os.environ['HOME'] + '/parameters/config_buffet/dt/contractsize.csv'
        contractsize = pd.read_csv(p1, index_col=0)

        add = account_config[com]['add']
        reduce = account_config[com]['reduce']
        accounts = self.accounts
        if len(accounts) == 0:
            accounts0 = list(account_config[com]['accounts'])
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
                
                try:
                    config = account_config[acc.combo]
                except:
                    logger.warning(f"{acc.parameter_name}:获取config错误!")
                    
                # 权益获取，获取不到跳过
                try:
                    eq = acc.get_equity()
                    equity = acc.adjEq
                except:
                     logger.warning(f"{acc.parameter_name}:获取equity错误!")
                    

                if equity is np.nan:
                    logger.warning(f"{acc.parameter_name}:没有获取到当前equity!")
                    continue
                else:
                    pass

                # 仓位获取，获取不到跳过
                try:
                    acc.get_account_position()
                    now_pos = acc.position
                    now_pos.set_index('coin', inplace=True)
                    now_pos = now_pos[now_pos['MV'] > min(config['min_select_u'], equity * config['min_select_ratio'])]
                    coins = list(now_pos.index)
                    logger.info(f"{acc.parameter_name}:当前获取到的position为：{now_pos.to_dict()}")

                except:
                    logger.warning(f"{acc.parameter_name}:没有获取最近10分钟position!")
                    continue

                # mr获取，获取不到跳过
                mr = acc.mr if hasattr(acc, "mr") else acc.get_mgnRatio()
                if mr != {}:
                    logger.info(f"{acc.parameter_name}:当前获取到的mr为：{mr}")

                else:
                    logger.warning(f"{acc.parameter_name}:没有获取到当前的mr。")

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

                    logger.info(f"{acc.parameter_name}:非新账户，初始化目前已经持有币的parameter参数。")
                    # print(acc,'老账户,将目前持有的币初始化参数')
                    now_pos['side'] = now_pos[['side']].applymap(lambda x: 1 if x == 'long' else 0)
                    parameter.loc[coins, 'account'] = acc.parameter_name
                    parameter.loc[coins, 'portfolio'] = 0
                    parameter.loc[coins, 'open'] = 2
                    parameter.loc[coins, 'closemaker'] = config['closemaker'][0]
                    parameter.loc[coins, 'position_multiple'] = config['position_multiple']
                    parameter.loc[coins, 'chase_tick'] = config['chase_tick']
                    parameter.loc[coins, 'position'] = now_pos['position']
                    parameter.loc[coins, 'is_long'] = now_pos['side']

                # 新账户，所有参数默认为空
                else:
                    logger.info(f"{acc.parameter_name}:新账户，初始化parameter，参数默认为空。")
                    # print(acc,'新账户，所有参数默认为空') 

                '''
                总MV%超限减仓
                '''
                now_mv = now_pos['MV%'].sum()

                if now_mv > config['accounts'][acc.parameter_name][0] * config['max_mv_multiple']:
                    mv_plus = now_mv - config['accounts'][acc.parameter_name][0] * config['reduce_mv_multiple']
                    logger.info(
                        f"{acc.parameter_name}:总仓位为：{now_mv} 大于 {config['accounts'][acc.parameter_name][0] * config['max_mv_multiple']}，进行总仓位超限减仓，{mv_plus}%的仓位将会被减掉。")
                    if mv_plus > 0:
                        rank = dict(sorted(add.items(), key=lambda x: x[1]))
                        hold_coin = []
                        for i in list(rank.keys()):
                            if i in coins:
                                hold_coin.append(i)

                        if len(hold_coin) == 0:
                            logger.warning(f"{acc.parameter_name}:总仓位超限，但是没有指定排序，所以将不会执行仓位超限减仓！")
                        for c in hold_coin:
                            if mv_plus > 0:
                                ava_reduce = now_pos.loc[c, 'MV%']
                                if ava_reduce <= mv_plus:
                                    parameter.loc[c, 'portfolio'] = -1
                                    spread = acc.get_spreads(c)
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
                                    logger.info(f"{acc.parameter_name}:{c}目前mv为{ava_reduce}%，小于{mv_plus}%,该币将会被一档减仓至0")
                                    mv_plus -= ava_reduce
                                else:
                                    parameter.loc[c, 'portfolio'] = -2
                                    parameter.loc[c, 'position'] -= now_pos.loc[c, 'position'] * mv_plus / now_pos.loc[
                                        c, 'MV%']
                                    logger.info(f"{acc.parameter_name}:{c}目前mv为{ava_reduce}%，该币将会被二档减仓至{ava_reduce - mv_plus}%。")
                                    mv_plus -= ava_reduce


                            else:
                                break
                else:
                    logger.info(
                        f"{acc.parameter_name}:目前仓位为：{now_mv}小于仓位上限{config['accounts'][acc.parameter_name][0] * config['max_mv_multiple']}，总仓位没有超限。")

                    '''
                    加仓
                    '''

                    if add == {} or sum(add.values()) == 0:
                        logger.warning(f"{acc.parameter_name}:可选加仓币数为{add},不执行加仓操作。")

                    else:
                        if min(mr.values()) < config['accounts'][acc.parameter_name][1]:
                            logger.warning(f"{acc.parameter_name}:目前账户有合约对应的mr低于{config['accounts'][acc.parameter][1]},不执行加仓操作。")

                        else:
                            # 计算可加仓mv%
                            uplimit_mv = config['accounts'][acc.parameter_name][0]
                            res_mv = max(0, uplimit_mv - now_mv)
                            if res_mv <= 0:
                                logger.info(f"{acc.parameter_name}:剩余可加仓mv为：{res_mv}%，不执行加仓操作。")

                            # 如果可以加仓，遍历需要加仓的币
                            else:
                                logger.info(f"{acc.parameter_name}:剩余可加仓mv为：{res_mv}%，执行加仓操作。")
                                for coin in list(add.keys()):
                                    coin_price = acc.get_coin_price(coin=coin, kind=acc.kind_master)
                                    spread = acc.get_spreads(coin)

                                    if acc.master.split('_')[1] == 'usd' and acc.exchange_master in ["okx", "okex", "okex5", "ok", "o", "okexv5"]:
                                        master = 'okex-' + acc.master.split('_')[1] + '-' + \
                                                 acc.master.split('_')[2]
                                        coin_price = contractsize.loc[coin.upper(), master]

                                    elif acc.master.split('_')[1] == 'usd' and acc.exchange_master not in [
                                        "okx", "okex", "okex5", "ok", "o", "okexv5"]:
                                        master = acc.master.replace('_', '-')
                                        coin_price = contractsize.loc[coin.upper(), master]

                                    else:
                                        print(acc, 'u本位，币价为实时价格')
                                        pass

                                    # 已持有币加仓
                                    if coin in list(now_pos.index):

                                        if res_mv * add[coin] / sum(list(add.values())) * equity / coin_price / 100 > 0:
                                            parameter.loc[coin, 'position'] += res_mv * add[coin] / sum(
                                                list(add.values())) * equity / coin_price / 100
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
                                            else:
                                                parameter.loc[coin, 'open'] = config['open'][0]

                                            logger.info(
                                                f"{acc.parameter_name}:{coin},'已持有币加仓，剩余新加比例为{add[coin] / sum(list(add.values())) * 100}%，新加position为:'{res_mv * add[coin] / sum(list(add.values())) * equity / coin_price / 100}")
                                        else:
                                            logger.info(
                                                f"{acc.parameter_name}:{coin},'已持有币加仓，剩余新加比例为{add[coin] / sum(list(add.values())) * 100}%，新加position为0")

                                    # 新币加仓       
                                    else:
                                        if add[coin] != 0:
                                            parameter.loc[coin, 'position'] = res_mv * add[coin] / sum(
                                                list(add.values())) * equity / coin_price / 100
                                            parameter.loc[coin, 'account'] = acc.parameter_name
                                            parameter.loc[coin, 'portfolio'] = 1
                                            parameter.loc[coin, 'closemaker'] = config['closemaker'][0]
                                            parameter.loc[coin, 'position_multiple'] = config['position_multiple']
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
                                            else:
                                                parameter.loc[coin, 'open'] = config['open'][0]

                                            logger.info(
                                                f"{acc.parameter_name}:{coin},'新币加仓，剩余新加比例为{add[coin] / sum(list(add.values())) * 100}%,新加position为：{res_mv * add[coin] / sum(list(add.values())) * equity / coin_price / 100}")
                                        else:
                                            logger.info(
                                                f"{acc.parameter_name}:{coin},'新币加仓，剩余新加比例为{add[coin] / sum(list(add.values())) * 100}%,新加position为0")

                    '''
                    减仓
                    '''
                    if reduce == {}:
                        logger.warning(f"{acc.parameter_name}:可选减仓币数为{reduce},不执行减仓操作。")

                    else:
                        logger.info(f"{acc.parameter_name}:可选减仓币数大于0，执行减仓操作。")
                        for coin in reduce.keys():
                            try:
                                _ = now_pos.loc[coin]['MV%']
                            except:
                                now_pos.loc[coin, 'MV%'] = 0
                                logger.warning(f"{acc.parameter_name}:目前没有持有{coin},该币减仓将不会执行")

                            coin_price = acc.get_coin_price(coin=coin, kind=acc.kind_master)
                            spread = acc.get_spreads(coin)

                            if acc.master.split('_')[1] == 'usd' and acc.exchange_master in ["okx", "okex",
                                                                                                         "okex5", "ok",
                                                                                                         "o", "okexv5"]:
                                master = 'okex-' + acc.master.split('_')[1] + '-' + acc.master.split('_')[2]
                                coin_price = contractsize.loc[coin.upper(), master]

                            elif acc.master.split('_')[1] == 'usd' and acc.exchange_master not in ["okx",
                                                                                                               "okex",
                                                                                                               "okex5",
                                                                                                               "ok",
                                                                                                               "o",
                                                                                                               "okexv5"]:
                                master = acc.master.replace('_', '-')
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
                                logger.info(f"{acc.parameter_name}:{coin}的目标仓位为{reduce[coin]}，该币将减仓至0，同时修改closemaker")

                            # 二档减仓
                            elif reduce[coin] < now_pos.loc[coin]['MV%']:
                                target_pos = reduce[coin] / now_pos.loc[coin, 'MV%'] * now_pos.loc[coin]['position']
                                parameter.loc[coin, 'position'] = target_pos
                                parameter.loc[coin, 'portfolio'] = -2
                                logger.info(f"{acc.parameter_name}:{coin}的目标仓位为{reduce[coin]}，该币将二档减仓至目标仓位")

                            # 不操作
                            else:
                                logger.info(
                                    f"{acc.parameter_name}:{coin}的目前仓位为{now_pos.loc[coin]['MV%']}，小于目标仓位{reduce[coin]},该币将不执行减仓操作。")

                #    计算fragment,fragment_min，closemaker2,coin

                for c in parameter.index:
                    coin_price = acc.get_coin_price(coin=c, kind=acc.kind_master)
                    spread = acc.get_spreads(c)

                    if acc.master.split('_')[1] == 'usd' and acc.exchange_master in ["okx", "okex", "okex5",
                                                                                                 "ok", "o", "okexv5"]:

                        master = 'okex-' + acc.master.split('_')[1] + '-' + acc.master.split('_')[2]
                        coin_price = contractsize.loc[c.upper(), master]

                    elif acc.master.split('_')[1] == 'usd' and acc.exchange_master not in ["okx", "okex",
                                                                                                       "okex5", "ok",
                                                                                                       "o", "okexv5"]:

                        master = acc.master.replace('_', '-')
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
                    else:
                        parameter.loc[c, 'closemaker2'] = config['closemaker2'][0]
                        
                    parameter.loc[c, 'coin'] = c+acc.contract_master.replace('future',config['future_date'][0])

                    parameter.loc[c, 'master_pair'] = parameter.loc[c, 'coin']
                    parameter.loc[c, 'slave_pair'] = c+acc.contract_slave.replace('future',config['future_date'][0])

                    try:
                        parameter.loc[c, 'position2'] = max(2 * parameter.loc[c, 'position'],
                                                            now_pos.loc[c, 'position'])
                    except:
                        logger.info(f"{acc.parameter_name}:{c}的目前仓位没有获取到，该币二档将为一档的2倍！")
                        parameter['position2'] = parameter['position'] * 2
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
                    all_parameter[acc.parameter_name] = parameter
                else:
                    logger.warning(f"{acc.parameter_name}:parameter为空！")

            if len(all_parameter) > 0:
                logger.info(f"开始保存到本地！")
                excel_name = self.save_excel(all_parameter=all_parameter, p="/jupyter/data/buffet/buffet2_result")
                logger.info(f"parameter保存本地成功！")
                if upload == True:
                    logger.info(f"开始上传到github!")
                    self.upload_parameter(excel_name, path="/.git-credentials")
                    logger.info(f"parameter上传github成功！")
                else:
                    print(123)
                    pass
            else:
                print('所有parameter表为空')
                logger.warning(f"所有账户parameter为空！")

        logger.handlers.clear()
        return all_parameter
