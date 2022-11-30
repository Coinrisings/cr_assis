import configparser, os
from accountBase import AccountData
config = configparser.ConfigParser()
config.read(f"{os.environ['HOME']}/config_buffet/accountdata.ini")
accounts = []
for key, values in config["accountdata"].items():
    exec(f'''{key} = {values}
    ''')
    accounts.append(eval(key))
print(config.options("accountdata"))
