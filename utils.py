import json
import os

class Config:
    def __init__(self, config_file=None):
        if config_file is None:
            # Construct the path to 'config.json' by moving one directory up from the current directory
            config_file = os.path.join(os.path.dirname(__file__), '..', 'config.json')
            config_file = os.path.abspath(config_file)  # Get the absolute path for safety

        # Open and load the JSON configuration file
        with open(config_file) as f:
            self.config = json.load(f)

    def get_s3_config(self):
        return self.config.get('s3', {})

    def get_elasticsearch_config(self):
        return self.config.get('elasticsearch', {})
