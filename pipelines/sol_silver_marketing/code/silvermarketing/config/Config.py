from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, bronze: str=None, silver: str=None, **kwargs):
        self.spark = None
        self.update(bronze, silver)

    def update(self, bronze: str="bronze", silver: str="silver", **kwargs):
        prophecy_spark = self.spark
        self.bronze = bronze
        self.silver = silver
        pass
