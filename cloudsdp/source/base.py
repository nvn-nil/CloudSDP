from abc import ABC, abstractmethod


class DataSource(ABC):
    @abstractmethod
    def extract(self):
        pass


class JsonDataSource(DataSource):
    def extract(self):
        pass
