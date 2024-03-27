from typing import Dict, List
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests


def search_movie(query: str, api_key: str, year: int = None):
    """
    Função buscar os dados básicos de um filme de acordo com o parâmetro 
    query (EX: Titulo do filme)

    args: query(str)*: Parâmetro para busca (EX: Titulo do filme)
          api_key (str)*: Api key
          year (int): Ano de lançamento do filme

    return: json
    """
    BASE_URL: str = 'https://api.themoviedb.org/3/search/movie'
    existing_params = {
        "include_adult": "false",
        "language": "en-US",
        "page": "1"
    }
    new_params = {"query": query}

    if year:
        new_params["year"] = year    
    all_params = {**existing_params, **new_params}
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    try:
        response = requests.get(BASE_URL, headers=headers, params= all_params)
        if response.status_code == 200:
            return response.json()["results"]
        else:
            return None
    except:
        return None

def search_details_movie(movie_id: int, api_key: str):
    BASE_URL: str = f'https://api.themoviedb.org/3/movie/{movie_id}'
    params = {
        "language": "en-US",
    }

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    return requests.get(BASE_URL, headers=headers, params= params).json()
    

if __name__ == '__main__':
    import os

    from dotenv import load_dotenv

    load_dotenv(dotenv_path="env\.env")
    api_key = os.getenv("TMDB_AUTORIZATION")

    movie_id = search_movie(query = "10 Cloverfield Lane", year=2016, api_key=api_key)[0]["id"]
    print(movie_id)
    response = search_details_movie(movie_id = movie_id, api_key=api_key)
    print(response)