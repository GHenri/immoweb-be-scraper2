import os
import json

dir = os.path.join(os.path.dirname(__file__), 'config.cfg')
with open(dir) as json_file:
    config = json.load(json_file)
