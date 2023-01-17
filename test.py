import datetime, requests, zipfile, io
import pandas as pd

# 溢价指数
dic={'ETHUSDT':'prem_ethusdt',
    'ETHBUSD':'prem_ethbusd',
    'BTCUSDT':'prem_btcusdt',
    'BTCBUSD':'prem_btcbusd'}

for i in dic.keys():
    date = datetime.datetime.strptime("2022-12-01", "%Y-%m-%d").date()
    url = "https://data.binance.vision/data/futures/um/daily/premiumIndexKlines/"+i+"/8h"
    curr_date = datetime.date.today()
    a=[]
    df=pd.DataFrame()
    while date < curr_date:
        date_str = date.strftime("%Y-%m-%d")
        file = f"{url}/"+i+"-8h-"+date_str+".zip"
        date = date + datetime.timedelta(days=3)
        # try:
        res=requests.get(file)
        with zipfile.ZipFile(io.BytesIO(res.content), "r") as zip_file:
            tmp = zip_file.open(i+"-8h-"+date_str+".csv")
            tmp_df= pd.read_csv(tmp, header=None)
            a.append(date_str)

        # except:
        #     tmp_df=pd.DataFrame()
        df=pd.concat([df,tmp_df])
    df.columns=['open_time', 'open', 'high', 'low', 'close', 'volume','close_time', 'quote_volume', 'count', 
                'taker_buy_volume','taker_buy_quote_volume', 'ignore']
    df.drop_duplicates('open_time',inplace=True)
    df=df.set_index('open_time').drop('open_time',axis=0)
    df.index=[datetime.datetime.utcfromtimestamp(float(i)/1000).strftime("%Y-%m-%d %H:%M:%S") for i in df.index]
    df.index=pd.to_datetime(df.index)
    df.index=df.index+pd.Timedelta('8h')
    df.to_csv(r'/home/luc/jupyter/data/'+dic[i]+'.csv')