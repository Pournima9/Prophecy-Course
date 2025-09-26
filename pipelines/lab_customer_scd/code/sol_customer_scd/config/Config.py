from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, source_path: str=None, bronze: str=None, dt: str=None, **kwargs):
        self.spark = None
        self.update(source_path, bronze, dt)

    def update(
            self,
            source_path: str="dbfs:/course_lab/rainforest_raw_data",
            bronze: str="bronze",
            dt: str="2023-01-05",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.source_path = source_path
        self.bronze = bronze
        self.dt = dt
        pass
