# import sys
import configparser
# import definitions


class Configuration:
    def __init__(self):
        # lib read file (definitions.ROOT_DIR + "/settings/dev.ini")
        self.config = configparser.ConfigParser()

        # self.config.read(definitions.ROOT_DIR + "/settings/dev.ini") # for docker environment
        self.config.read(
            '/Users/thawaree/Documents/GitHub/zg-analytics-engine-python/src' + "/settings/dev.ini")

    def getConfig(self):
        return self.config


# config = Configuration().getConfig()
# env = config.get("AUTH_DATABASE", "URL")
# print('env =>', env)


