from typing import Dict, List, Union
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from api.consult_class import MovieAPI

import requests

class OMDBMovieAPI(MovieAPI):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def search_movie(self, title: str, year: int = None) -> Dict:
        """
        Função buscar os dados básicos de um filme de acordo com o parâmetro
        query (EX: Titulo do filme)

        args: title(str)*: Parâmetro para busca (EX: Titulo do filme)
            year (int): Ano de lançamento do filme

        return: json
        """
        BASE_URL: str = f"http://www.omdbapi.com/?apikey={self.api_key}&"
        params = {"t": title, "type": "movie"}

        if year:
            params["y"] = year

        try:
            response = requests.get(BASE_URL, params=params)
            if response.status_code == 200:
                return response.json()
            else:
                return None
        except:
            return None
        
    def get_api_dict(self):
        return {
            "imdbRating": {
                "row": "IMDB_Rating",
                "filter": 'float(value.replace(",", "")) if value and value != "N/A" else None',
            },
            "Metascore": {
                "row": "Metascore",
                "filter": 'float(value.replace(",", "")) if value and value != "N/A" else None',
            },
            "imdbVotes": {
                "row": "Votes",
                "filter": 'int(value.replace(",", "")) if value and value != "N/A" else None',
            },
            "Director": {
                "row": "Director",
            },
            "Year": {
                "row": "Year",
                "filter": 'datetime.strptime(value, "%Y").year if value and value != "N/A" else None',
            },
            "BoxOffice": {
                "row": "DomesticBox_office",
                "filter": "float(f'{re.sub(r'[$,]', '', value)}.0') if value and value != 'N/A' else None",
            },
            "Actors": {
                "row": "Cast",
            },
            "Genre": {"row": "Genre", "filter": 'value.split(",")[0]'},
        }
    
# def get_movie(title: str, api_key: str, year: int = None):
#     BASE_URL: str = f"http://www.omdbapi.com/?apikey={api_key}&"
#     params = {"t": title, "type": "movie"}

#     if year:
#         params["y"] = year

#     try:
#         response = requests.get(BASE_URL, params=params)
#         if response.status_code == 200:
#             return response.json()
#         else:
#             return None
#     except:
#         return None

# def get_omdb_dict():
#     return {
#         "imdbRating": {
#             "row": "IMDB_Rating",
#             "filter": 'float(value.replace(",", "")) if value and value != "N/A" else None',
#         },
#         "Metascore": {
#             "row": "Metascore",
#             "filter": 'float(value.replace(",", "")) if value and value != "N/A" else None',
#         },
#         "imdbVotes": {
#             "row": "Votes",
#             "filter": 'int(value.replace(",", "")) if value and value != "N/A" else None',
#         },
#         "Director": {
#             "row": "Director",
#         },
#         "Year": {
#             "row": "Year",
#             "filter": 'datetime.strptime(value, "%Y").year if value and value != "N/A" else None',
#         },
#         "BoxOffice": {
#             "row": "DomesticBox_office",
#             "filter": "float(f'{re.sub(r'[$,]', '', value)}.0') if value and value != 'N/A' else None",
#         },
#         "Actors": {
#             "row": "Cast",
#         },
#         "Genre": {"row": "Genre", "filter": 'value.split(",")[0]'},
#     }

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv(dotenv_path="env/.env")
    api_key = os.getenv("OMDB_KEY")

    omdb_api = OMDBMovieAPI(api_key)
    response = omdb_api.search_movie(title="10 Cloverfield Lane", year=2016)
    print(response)
