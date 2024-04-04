from typing import Dict, List, Union
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from api.consult_class import MovieAPI

import requests

class TMDBMovieAPI(MovieAPI):
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
        BASE_URL: str = "https://api.themoviedb.org/3/search/movie"
        existing_params = {"include_adult": "false", "language": "en-US", "page": "1"}
        new_params = {"query": title}

        if year:
            new_params["year"] = year
        all_params = {**existing_params, **new_params}
        headers = {"accept": "application/json", "Authorization": f"Bearer {self.api_key}"}

        try:
            response = requests.get(BASE_URL, headers=headers, params=all_params)
            if response.status_code == 200:
                json_data = response.json()
                if "results" in json_data and json_data["results"] and "id" in json_data["results"][0]:
                    movie_id = json_data["results"][0]["id"]
                    return self.search_details_movie(movie_id)
            else:
                return None
        except:
            return None
    
    def search_details_movie(self, movie_id: Union[int, str]) -> Dict:
        BASE_URL: str = f"https://api.themoviedb.org/3/movie/{movie_id}"
        params = {
            "language": "en-US",
        }

        headers = {"accept": "application/json", "Authorization": f"Bearer {self.api_key}"}

        return requests.get(BASE_URL, headers=headers, params=params).json()

    def get_api_dict(self):
        return {
            "budget": {
                "row": "production_cost",
            },
            "runtime": {
                "row": "Runtime",
            },
            "revenue": {
                "row": "WorldwideBox_office",
            },
            "release_date": {
                "row": "release_date",
                "filter": 'datetime.strptime(value, "%Y-%m-%d").month',
            },
        }


    
# def search_movie(title: str, api_key: str, year: int = None):
#     """
#     Função buscar os dados básicos de um filme de acordo com o parâmetro
#     query (EX: Titulo do filme)

#     args: title(str)*: Parâmetro para busca (EX: Titulo do filme)
#           api_key (str)*: Api key
#           year (int): Ano de lançamento do filme

#     return: json
#     """
#     BASE_URL: str = "https://api.themoviedb.org/3/search/movie"
#     existing_params = {"include_adult": "false", "language": "en-US", "page": "1"}
#     new_params = {"query": title}

#     if year:
#         new_params["year"] = year
#     all_params = {**existing_params, **new_params}
#     headers = {"accept": "application/json", "Authorization": f"Bearer {api_key}"}

#     try:
#         response = requests.get(BASE_URL, headers=headers, params=all_params)
#         if response.status_code == 200:
#             return response.json()["results"]
#         else:
#             return None
#     except:
#         return None


# def search_details_movie(movie_id: int, api_key: str):
#     BASE_URL: str = f"https://api.themoviedb.org/3/movie/{movie_id}"
#     params = {
#         "language": "en-US",
#     }

#     headers = {"accept": "application/json", "Authorization": f"Bearer {api_key}"}

#     return requests.get(BASE_URL, headers=headers, params=params).json()

# def get_tmdb_dict():
#     return {
#         "budget": {
#             "row": "production_cost",
#         },
#         "runtime": {
#             "row": "Runtime",
#         },
#         "revenue": {
#             "row": "WorldwideBox_office",
#         },
#         "release_date": {
#             "row": "release_date",
#             "filter": 'datetime.strptime(value, "%Y-%m-%d").month',
#         },
#     }
    

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv(dotenv_path="env/.env")
    api_key = os.getenv("TMDB_KEY")

    tmdb_api = TMDBMovieAPI(api_key)
    response = tmdb_api.search_movie(title="10 Cloverfield Lane", year=2016)
    print(response)
