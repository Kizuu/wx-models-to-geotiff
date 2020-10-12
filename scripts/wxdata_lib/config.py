import json
import os
import sys

directory = os.path.dirname(os.path.realpath(__file__))

try:
    with open(directory + '/../config.json') as f:
        data = json.load(f)
except:
    print("Error: Config file does not exist or is corrupt.")
    sys.exit(1)

config = data["config"]
levelMaps = data["levelMaps"]
models = data["models"]
