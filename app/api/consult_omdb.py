from typing import Dict, List
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests


def get_movie(title: str, api_key: str, year: int = None):
    BASE_URL: str = f'http://www.omdbapi.com/?apikey={api_key}&'
    params = {
        "t": title,
        "type": "movie"
    }

    if year:
        params["y"] = year 

    try:
        response = requests.get(BASE_URL, params= params)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except:
        return None
    

if __name__ == '__main__':
    import os

    from dotenv import load_dotenv

    load_dotenv(dotenv_path="env\.env")
    api_key = os.getenv("OMDB_KEY")

    movie_id = get_movie(title = "10 Cloverfield Lane", year=2016, api_key=api_key)
    print(movie_id)