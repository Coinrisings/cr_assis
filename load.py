import sys, os, datetime, glob, time, math, configparser, json, yaml
sys.path.append(f"{os.path.dirname(os.getcwd())}/cr_monitor")
from bokeh.io import output_notebook
output_notebook()
import pandas as pd
import numpy as np
with open(f"{os.environ['HOME']}/.cryptobridge/private_key.yml", "rb") as f:
    data = yaml.load(f, Loader= yaml.SafeLoader)
for info in data:
    if "mongo" in info.keys():
        os.environ["MONGO_URI"] = info['mongo']
        os.environ["INFLUX_URI"] = info['influx']
        os.environ["INFLUX_MARKET_URI"] = info['influx_market']