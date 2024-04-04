from abc import ABC, abstractmethod
from typing import Dict

class MovieAPI(ABC):
    @abstractmethod
    def search_movie(self, title: str, year: int = None) -> Dict:
        pass

    @abstractmethod
    def get_api_dict():
        pass