import yaml
from yaml.loader import SafeLoader
import json
from utils import *
from loadVkConnent import *
import logging
import logging_loki

logging.basicConfig(level=logging.DEBUG)



with open("config.yaml", "r") as stream:
    ConfigSystem = yaml.load(stream, Loader=SafeLoader)

with open('config_table.json') as json_file:
    ConfigTable = json.load(json_file)

ConfigGrafanaLoki = ConfigSystem['GrafanaLoki']
handler = logging_loki.LokiHandler(
    url= ConfigGrafanaLoki.get('URL'),
    tags=ConfigGrafanaLoki.get('TAG'),
    auth=(ConfigGrafanaLoki.get('USER'), ConfigGrafanaLoki.get('PASSWORD')),
    version=ConfigGrafanaLoki.get('VERSION')
)
logger = logger.addHandler(handler)

# основное выполнение
try:
    MP = ManagerPost(ConfigSystem,ConfigTable)
    MP.CommitPost()
except Exception as ex:
    MP.MessageErrorShare(ex)