from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, dt: str=None, source_path: str=None, bronze: str=None, **kwargs):
        self.spark = None
        self.update(dt, source_path, bronze)

    def update(
            self,
            dt: str="2023-01-05",
            source_path: str="dbfs:/course_lab/rainforest_raw_data",
            bronze: str="bronze",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.dt = dt
        self.source_path = source_path
        self.bronze = bronze
        pass
