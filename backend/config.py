# config.py
import json5

#Legge le impostazioni SQL

with open("config.jsonc") as f:
    CONFIG = json5.load(f)

HOME = CONFIG["HOME"]
MAX_LIMIT = CONFIG["MAX_LIMIT"]
DEMO_HANDLE = CONFIG["DEMO_HANDLE"]
DEMO_APP_PWD = CONFIG["DEMO_APP_PWD"]