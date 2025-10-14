import os

import yaml


def load_config():
    filedir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(filedir, 'config.yml'), 'r') as file:
        config = yaml.safe_load(file)
    return config
    
config = load_config()