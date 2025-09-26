from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, bronze: str=None, silver: str=None, gold: str=None, **kwargs):
        self.spark = None
        self.update(bronze, silver, gold)

    def update(self, bronze: str="bronze", silver: str="silver", gold: str="gold", **kwargs):
        prophecy_spark = self.spark
        self.bronze = bronze
        self.silver = silver
        self.gold = gold
        pass
