import json
from datetime import datetime
from typing import Dict, List, Union
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests
# from classes.consult_class import MovieAPI
# from schema.tmdb_schema import TmdbSchema
from api.classes.consult_class import MovieAPI
from api.schema.tmdb_schema import TmdbSchema


class TMDBMovieAPI(MovieAPI):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def search_movie(self, row: Dict, data_cache: Dict = None) -> json:
        """
        Função buscar os dados básicos de um filme de acordo com o parâmetro
        query (EX: Titulo do filme)

        args: Row: Linha do dataframe com as informações para serem utilizadas
              na busca

        return: json
        """
        movie_id = row["id"]

        BASE_URL: str = f"https://api.themoviedb.org/3/movie/{movie_id}"
        params = {
            "language": "en-US",
        }

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }
        try:
            response = requests.get(BASE_URL, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                with open("TMDB_response.json", "w") as f:
                        json.dump(data, f)
                # object_data = self.build_object(data)
                # print(object_data)
                return self.build_object(data)
            else:
                return None
        except:
            return None

    def build_object(self, api_response: json) -> TmdbSchema:
        """
        Função para transformar o retorno da api em um objeto do tipo TmdbSchema

        args: api_response(json): Json com o retorno da AOU e informações a serem
                                  transformadas em um objeto do tipo TmdbSchema

        return: TmdbSchema
        """
        row_list = self.get_api_dict()
        for key, value in list(api_response.items()):
            if key in row_list:
                if value is not None:
                    row_value = row_list[key]
                    rows = row_value["row"] if isinstance(row_value["row"], list) else [row_value["row"]]
                    for row in rows:
                        if "filter" in row_value:
                            filter_code = row_value["filter"]
                            api_response[row] = eval(filter_code)
                        else:
                            api_response[row] = value
                else:
                    return None
                
        return TmdbSchema(
            budget = api_response["budget"],
            runtime = api_response["runtime"],
            revenue = api_response["revenue"],
            release_date = api_response["release_date"],
            vote_average = api_response["vote_average"],
            vote_count = api_response["vote_count"],
            id = api_response["id"],
            original_language = api_response["original_language"],
            Genre_1 = api_response["Genre_1"],
            Genre_2 = api_response["Genre_2"],
            Genre_3 = api_response["Genre_3"],
            popularity = api_response["popularity"],
            Production_Companies = api_response["Production_Companies"],
            Title = api_response["title"],
        )
    
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
            "vote_average": {
                "row": "vote_average",
            },
            "vote_count": {
                "row": "vote_count",
            },
            "id": {
                "row": 'id'
            },
            "original_language": {
                "row": 'original_language'
            },
            "genres": {
                "row": ['Genre_1', 'Genre_2', 'Genre_3'],
                "filter": "value[rows.index(row)]['name'] if rows.index(row) < len(value) else None"
            },
            "Genre_1": {
                "row": "Genre_1",
            },
            "Genre_2": {
                "row": "Genre_2",
            },
            "Genre_3": {
                "row": "Genre_3",
            },
            "popularity": {
                "row": 'popularity',
            },
            "Production_Companies": {
                "row": 'Production_Companies',
            },
            "production_companies": {
                "row": "Production_Companies",
                "filter": "value[0]['name'] if len(value) and value is not None else None"
            },
            "title":{
                "row": "Title"
            },
            "Title":{
                "row": "Title"
            }
        }


if __name__ == "__main__":
    import os
    import sys
    project_root = 'C:/Users/JPedr/OneDrive/Documentos/TCC/Projeto_2'
    sys.path.append(project_root)
    
    from consult import complete_df
    from dotenv import load_dotenv
    from app.pipeline.extract import read_file
    from app.pipeline.load import load_csv


    load_dotenv(dotenv_path="env/.env")
    api_key = os.getenv("TMDB_KEY")

    tmdb_api = TMDBMovieAPI(api_key)
    
    # df = read_file(path="C:/Users/JPedr/OneDrive/Documentos/TCC/Projeto_2/data/input/All Time Worldwide Box Office (filter).csv", delimiter= ";")
    df = read_file(path="C:/Users/JPedr/OneDrive/Documentos/TCC/Projeto_2/data/output/Box_Office DataBase(not_complete).csv", delimiter= ";")
    df = df.head(15)
    print(df)
    df = complete_df(df, [tmdb_api])
    print(df)

    load_csv(
        data_frame=df,
        output_path="C:/Users/JPedr/OneDrive/Documentos/TCC/Projeto_2/data/output",
        filename="Box_Office DataBase(not_complete)_test",
        delimiter= ";"
    )
    # response = tmdb_api.search_movie(title="10 Cloverfield Lane", year=2016)
