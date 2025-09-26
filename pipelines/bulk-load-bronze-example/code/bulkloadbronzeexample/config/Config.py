from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, dt: str=None, bronze: str=None, **kwargs):
        self.spark = None
        self.update(dt, bronze)

    def update(self, dt: str="2020-01-05", bronze: str="bronze", **kwargs):
        prophecy_spark = self.spark
        self.dt = dt
        self.bronze = bronze
        pass
