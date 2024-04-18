import json
import re
from datetime import datetime
from typing import Dict, List, Union
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
import time

import pandas as pd
import requests
from api.classes.consult_class import MovieAPI
from api.schema.omdb_schema import OmdbSchema
# from classes.consult_class import MovieAPI
# from schema.omdb_schema import OmdbSchema
from tqdm import tqdm


class OMDBMovieAPI(MovieAPI):
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
        
        # title = row["Title"] if row["Title"] is not None or row["Title"] != "NaN" or pd.notna(row["Title"]) else None
        # year = int(row["Year"]) if row["Year"] is not None or row["Year"] != "NaN" or pd.notna(row["Year"]) else None
        title = row["Title"] if row["Title"] is not None and pd.notna(row["Title"]) else None
        year = int(row["Year"]) if row["Year"] is not None and pd.notna(row["Year"]) else None
        if not title or not year: 
            print(row)
            return None

        BASE_URL: str = f"http://www.omdbapi.com/?apikey={self.api_key}&"
        params = {"t": title, "type": "movie"}

        if year:
            params["y"] = year
        
        try:
            response = requests.get(BASE_URL, params=params)
            data = response.json()
            with open("OMDB_response.json", "w") as f:
                    json.dump(data, f)
            if response.status_code == 200 and 'Response' in data and data['Response'] == "True":
                return self.build_object(data)
            else:
                return None
        except:
            return None

    def build_object(self, api_response: json) -> OmdbSchema:
        """
        Função para transformar o retorno da api em um objeto do tipo OmdbSchema

        args: api_response(json): Json com o retorno da AOU e informações a serem
                                  transformadas em um objeto do tipo OmdbSchema

        return: OmdbSchema
        """
        row_list = self.get_api_dict()
        for key, value in api_response.items():
            if key in row_list:
                if value is not None:
                    row_value = row_list[key]
                    if "filter" in row_value:
                        filter_code = row_value["filter"]
                        api_response[key] = eval(filter_code)
                    else:
                        api_response[key] = value
                else:
                    return None

        return OmdbSchema(
            imdbRating=api_response["imdbRating"],
            Metascore=api_response["Metascore"],
            Director=api_response["Director"],
            BoxOffice=api_response["BoxOffice"],
            Actors=api_response["Actors"],
        )

    def get_api_dict(self):
        return {
            "Actors": {
                "row": "Cast",
                "filter": "value if value and value != 'N/A' and value != 'NaN' and value != 'nan' else None"
            },
            "Director": {
                "row": "Director",
                "filter": "value if value and value != 'N/A' and value != 'NaN' else None"
            },
            "BoxOffice": {
                "row": "DomesticBox_office",
                "filter": "float(f'{re.sub(r'[$,]', '', value)}.0') if value and value != 'N/A' else None",
            },
            "imdbRating": {
                "row": "IMDB_Rating",
                "filter": 'float(value.replace(".", "")) if value and value != "N/A" else None',
            },
            "Metascore": {
                "row": "Metascore",
                "filter": '(float(value.replace(".", "")) / 10) if (value and value != "N/A" and float(value.replace(",", "")) > 100) else (float(value.replace(",", "")) if (value and value != "N/A") else None)',
            },
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
    api_key = os.getenv("OMDB_KEY")

    omdb_api = OMDBMovieAPI(api_key)
    
    df = read_file(path="C:/Users/JPedr/OneDrive/Documentos/TCC/Projeto_2/data/output/Box_Office DataBase(not_filtered).csv", delimiter= ";")
    df = df.head(15)
    print(df)
    df = complete_df(df, [omdb_api])
    print(df)

    load_csv(
        data_frame=df,
        output_path="C:/Users/JPedr/OneDrive/Documentos/TCC/Projeto_2/data/output",
        filename="Box_Office DataBase(not_complete)_test_omdb",
        delimiter= ";"
    )

    # import os

    # from dotenv import load_dotenv

    # load_dotenv(dotenv_path="env/.env")
    # api_key = os.getenv("OMDB_KEY")

    # omdb_api = OMDBMovieAPI(api_key)
    # response = omdb_api.search_movie(title="Ant-Man and the Wasp", year=2018)
    # print(response)
