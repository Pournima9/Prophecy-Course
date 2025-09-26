from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, silver: str=None, gold: str=None, **kwargs):
        self.spark = None
        self.update(silver, gold)

    def update(self, silver: str="silver", gold: str="gold", **kwargs):
        prophecy_spark = self.spark
        self.silver = silver
        self.gold = gold
        pass
