from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, bronze: str=None, **kwargs):
        self.spark = None
        self.update(bronze)

    def update(self, bronze: str="bronze", **kwargs):
        prophecy_spark = self.spark
        self.bronze = bronze
        pass
