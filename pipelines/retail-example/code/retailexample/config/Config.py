from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, gold: str=None, **kwargs):
        self.spark = None
        self.update(gold)

    def update(self, gold: str="gold", **kwargs):
        prophecy_spark = self.spark
        self.gold = gold
        pass
