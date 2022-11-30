from pymongo import MongoClient
from influxdb import InfluxDBClient
import redis, os, yaml
import urllib3
urllib3.disable_warnings()

class ConnectData(object):
    def __init__(self) -> None:
        self.mongo_url = self.load_mongo_url()
        self.mongo_clt = MongoClient(self.mongo_url)
        self.influx_json = None
        self.influx_clt = None
    
    def load_mongo_url(self) -> None:
        user_path = os.path.expanduser('~')
        cfg_path = os.path.join(user_path, '.cr_assis')
        if not os.path.exists(cfg_path):
            os.mkdir(cfg_path)
        with open(os.path.join(cfg_path, 'mongo_url.yml')) as f:
            ret = yaml.load(f, Loader = yaml.SafeLoader)
            for item in ret:
                if item["name"] == "mongo":
                    return item["url"]
    
    def load_influxdb(self, database = "ephemeral") -> None:
        db = "DataSource"
        coll = "influx"
        influx_json = self.mongo_clt[db][coll].find_one({"_id" : {"$regex" : f".*{database}$"}})
        client = InfluxDBClient(host = influx_json["host"],
                port = influx_json["port"],
                username = influx_json["username"],
                password = influx_json["password"],
                database = influx_json["database"],
                ssl = influx_json["ssl"])
        self.influx_json = influx_json
        self.influx_clt = client
        
    def load_redis(self, database = "dratelimit_new") -> None:
        db = "DataSource"
        coll = "redis"
        redis_json = self.mongo_clt[db][coll].find_one({"_id" : {"$regex" : f".*{database}$"}})
        pool = redis.ConnectionPool(host = redis_json["host"], password = redis_json["password"])
        self.redis_clt = redis.Redis(connection_pool = pool)
        self.redis_json = redis_json
        
    def get_redis_data(self, key: str) -> dict:
        """key is exchange/base currency-quote currency, for example: binance/btc-usdt
        
        return dict: {b'string': b'number'}"""
        key = bytes(key.lower(), encoding = "utf8")
        if not hasattr(self, "redis_clt"):
            self.load_redis()
        data = self.redis_clt.hgetall(key)
        return data
    