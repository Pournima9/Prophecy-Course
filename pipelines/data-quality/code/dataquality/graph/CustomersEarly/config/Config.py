from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            source_path: str="dbfs:/course_lab/rainforest_raw_data",
            dt: str="2023-06-01",
            **kwargs
    ):
        self.source_path = source_path
        self.dt = dt
        pass

    def update(self, updated_config):
        self.source_path = updated_config.source_path
        self.dt = updated_config.dt
        pass

Config = SubgraphConfig()
