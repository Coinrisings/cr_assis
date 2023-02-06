import datetime, requests, datetime, json
import pandas as pd

url = "https://api.thegraph.com/subgraphs/name/ensdomains/ens"
headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
data = []
domains = []
last_id = ""
num = 0
while len(data) == 0 or len(domains) == 1000:
    sql = """query overviewCharts {
                domains(first: 1000, orderBy: id, orderDirection: asc, where: {id_gt: "$last_id"}) {
                    labelName
                    labelhash
                    name
                    id
                }
            }""".replace("$last_id", last_id)
    payload = {"query": sql}
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200 and "data" in response.json().keys() and "domains" in response.json()["data"].keys():
        domains = response.json()['data']['domains']
        data += domains
        if len(domains) > 0:
            last_id = domains[-1]["id"]
    else:
        domains = []
    num += 1
    print(f"{datetime.datetime.now()} <{num}>")
    if num >=5:
        break
with open("/Users/ssh/Documents/GitHub/Hello-World-/data/ens_token.json", "w") as f:
    json.dump(data, f)


data = pd.read_excel("/Users/ssh/Documents/MEGA/SSH/coinrising/DeFi/others/ens.xlsx")
total = len(data)
for i in data.index:
    token_id = data.loc[i, "nft_token_id"]
    url = f"https://metadata.ens.domains/mainnet/0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85/{token_id}"
    response = requests.get(url)
    if response.status_code == 200:
        info = response.json()
        if "name" in info.keys():
            data.loc[i, "name"] = info["name"]
    print(f"{datetime.datetime.now()}: <{i}/{total}>")
data = []
end_time = datetime.datetime(2023,2,3,1,12,40)
headers = {
    "accept": "application/json",
    "API-KEY": "vn7BB7itj62GewochjAjotqmvX1ztjmElhlka4abE7LSN8vYKjenPtNUq540Avy3"
}
while end_time >= datetime.datetime(2022,8,27,0,0,0):
    start_time = end_time + datetime.timedelta(days = -1)
    end = str(end_time).split(".")[0].replace(" ", "%20").replace(":", "%3A")
    start = str(start_time).split(".")[0].replace(" ", "%20").replace(":", "%3A")
    url = f"https://api.footprint.network/api/v2/nft/collection/transactions?chain=BNB%20Chain&collection_contract_address=0xE3b1D32e43Ce8d658368e2CBFF95D57Ef39Be8a6&start_time={start}&end_time={end}"
    response = requests.get(url, headers=headers)
    data = data + response.json()['data']
    num = len(response.json()["data"])
    if num >= 50:
        end_time = datetime.datetime.strptime(response.json()["data"][-1]["block_timestamp"][:19], "%Y-%m-%dT%H:%M:%S")
    else:
        end_time = start_time + datetime.timedelta(minutes= -1)
    print("\r", f"{datetime.datetime.now()} <{end_time}>: {num}", end = "")
print(response.text)


import requests

token_id = "74212330472190394778944248169764675226824338582996915647193419151477897914046"
url = f"https://metadata.ens.domains/mainnet/0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85/{token_id}"

headers = {
    "accept": "application/json",
    "API-KEY": "hY+jgJwzkA90/josXolSrC1SuGU4V//GbcErlVoDqr/5yKT8JCMW3oDVg+kfMPxE"
}

response = requests.get(url, headers=headers)
data = response.json()
print(response.text)


# bg003 = AccountBase(deploy_id="bg_bg003@dt_okex_uswap_okex_cfuture_btc")
# fso = FsoPnl(accounts = [bg003])
# fso.get_open_time(bg003)
# print(bg003.start)
# # 溢价指数
# dic={'ETHUSDT':'prem_ethusdt',
#     'ETHBUSD':'prem_ethbusd',
#     'BTCUSDT':'prem_btcusdt',
#     'BTCBUSD':'prem_btcbusd'}

# for i in dic.keys():
#     date = datetime.datetime.strptime("2022-12-01", "%Y-%m-%d").date()
#     url = "https://data.binance.vision/data/futures/um/daily/premiumIndexKlines/"+i+"/8h"
#     curr_date = datetime.date.today()
#     a=[]
#     df=pd.DataFrame()
#     while date < curr_date:
#         date_str = date.strftime("%Y-%m-%d")
#         file = f"{url}/"+i+"-8h-"+date_str+".zip"
#         date = date + datetime.timedelta(days=3)
#         # try:
#         res=requests.get(file)
#         with zipfile.ZipFile(io.BytesIO(res.content), "r") as zip_file:
#             tmp = zip_file.open(i+"-8h-"+date_str+".csv")
#             tmp_df= pd.read_csv(tmp, header=None)
#             a.append(date_str)

#         # except:
#         #     tmp_df=pd.DataFrame()
#         df=pd.concat([df,tmp_df])
#     df.columns=['open_time', 'open', 'high', 'low', 'close', 'volume','close_time', 'quote_volume', 'count', 
#                 'taker_buy_volume','taker_buy_quote_volume', 'ignore']
#     df.drop_duplicates('open_time',inplace=True)
#     df=df.set_index('open_time').drop('open_time',axis=0)
#     df.index=[datetime.datetime.utcfromtimestamp(float(i)/1000).strftime("%Y-%m-%d %H:%M:%S") for i in df.index]
#     df.index=pd.to_datetime(df.index)
#     df.index=df.index+pd.Timedelta('8h')
#     df.to_csv(r'/home/luc/jupyter/data/'+dic[i]+'.csv')