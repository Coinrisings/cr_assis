import datetime, requests, datetime, json
import pandas as pd

url = "https://api.uniswap.org/v1/graphql"
sql = "query TokenPrice() {\n  token(chain: ETHEREUM, address: 0x15d4c048f83bd7e37d49ea4c83a07267ec4203da) {\n    id\n    address\n    chain\n    market(currency: USD) {\n      id\n      price {\n        id\n        value\n        __typename\n      }\n      priceHistory(duration: YEAR) {\n        id\n        timestamp\n        value\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}"
headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
payload = {"query": sql}
response = requests.post(url, json=payload, headers=headers)


start = datetime.datetime(2022,11,3,0,0,0)
end = datetime.datetime(2022,11,5,0,0,0)
timestamp = start
result = {"prices": [], "market_caps": [], "total_volumes": []}
while timestamp <= end:
    unix_start = int(datetime.datetime.timestamp(timestamp))
    unix_end = int(datetime.datetime.timestamp(min(timestamp + datetime.timedelta(days = 1), end)))
    url = f"https://api.coingecko.com/api/v3/coins/ethereum/contract/0x15d4c048f83bd7e37d49ea4c83a07267ec4203da/market_chart/range?vs_currency=usd&from={unix_start}&to={unix_end}"
    response = requests.get(url)
    if response.status_code == 200:
        ret = response.json()
        for name, data in ret.items():
            result[name] = result[name] + ret[name]
    else:
        print(response.text)
    timestamp = timestamp + datetime.timedelta(days = 1)
data = pd.DataFrame(result['prices'])
data.columns = ["unix", "price"]
data['time'] = data['unix'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000))
print(data)