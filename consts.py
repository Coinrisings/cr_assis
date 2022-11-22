import configparser, os
# path = research.__path__.__dict__["_path"][0]
from accountData import AccountData
config = configparser.ConfigParser()
config.read(f"{os.environ['HOME']}/config_buffet/accountdata.ini")
accounts = []
for key, values in config["accountdata"].items():
    exec(f'''{key} = {values}
    ''')
    accounts.append(eval(key))
print(config.options("accountdata"))
