import os
import re
from datetime import datetime
from typing import Dict, List

import pandas as pd
from api.consult_omdb import get_movie, get_omdb_dict
from api.consult_tmdb import search_details_movie, search_movie, get_tmdb_dict
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv(dotenv_path="env/.env")


def complete_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Função para completar as informações vazias do dataframe,
    através de consultas a API's.
    
    args: df(pd.DataFrame)*: dataframe a ser completado

    return: df(pd.DataFrame) : dataframe completo
    """
    ROWS_TMDB: Dict = get_tmdb_dict()
    ROWS_OMDB: Dict = get_omdb_dict()

    tmdb_data_cache = {}
    omdb_data_cache = {}
    rows_to_update = []
    
    for index, row in tqdm(
        df.iterrows(), total=len(df), desc="Consultando API"
    ):
        tmdb_columns_to_update = [value['row'] for key, value in ROWS_TMDB.items() if pd.isnull(row.get(value['row']))]
        omdb_columns_to_update = [value['row'] for key, value in ROWS_OMDB.items() if pd.isnull(row.get(value['row']))]

        if tmdb_columns_to_update or omdb_columns_to_update:
            rows_to_update.append(index)

        tmdb_data_needed = any(tmdb_columns_to_update) and not tmdb_data_cache.get(index)
        omdb_data_needed = any(omdb_columns_to_update) and not omdb_data_cache.get(index)

        if tmdb_data_needed:
            tmdb_data_cache[index] = search_tmdb_data(row)
        if omdb_data_needed:
            omdb_data_cache[index] = search_omdb_data(row)
        
    for index in tqdm(
        rows_to_update, total=len(rows_to_update), desc="Atualizando Dataframe"
    ):
        row = df.loc[index]
        if index in tmdb_data_cache and tmdb_data_cache[index] is not None:
            df = insert_info_movie(tmdb_data_cache[index], ROWS_TMDB, df, index, row)

        if index in omdb_data_cache and omdb_data_cache[index] is not None:
            df = insert_info_movie(omdb_data_cache[index], ROWS_OMDB, df, index, row)

    return df

def search_tmdb_data(row: pd.Series) -> Dict:
    """
    Busca informações sobre um filme na API TMDB.

    Args:
        row (pd.Series): Uma linha de um DataFrame contendo informações sobre o filme, como título e ano.

    Returns:
        Dict: Um dicionário contendo as informações do filme retornadas pela API TMBD, se a resposta for bem-sucedida.
              Caso contrário, retorna None.
    """
    data_tmdb = search_movie(
        title=row["Title"],
        api_key=os.getenv("TMDB_AUTORIZATION"),
        year=row["Year"],
    )
    if data_tmdb and data_tmdb[0]["id"]:
        return search_details_movie(
            movie_id=data_tmdb[0]["id"],
            api_key=os.getenv("TMDB_AUTORIZATION"),
        )
    pass

def search_omdb_data(row: pd.Series) -> Dict:
    """
    Busca informações sobre um filme na API OMDB.

    Args:
        row (pd.Series): Uma linha de um DataFrame contendo informações sobre o filme, como título e ano.

    Returns:
        Dict: Um dicionário contendo as informações do filme retornadas pela API OMDB, se a resposta for bem-sucedida.
              Caso contrário, retorna None.
    """
    data_omdb = get_movie(
        title=row.get("Title"),
        year=int(row.get("Year")),
        api_key=os.getenv("OMDB_KEY"),
    )
    if data_omdb and data_omdb["Response"]:
        return data_omdb
    pass

def insert_info_movie(response: Dict, row_list: Dict, df: pd.DataFrame, index: int, row: List) -> pd.DataFrame:
    """
    Função para inserir os dados retornados da api no dataframe

    args: response(Dict)*: Retorno da API
          row_list (Dict[str])*: Dicionario com os parâmetros de rotorno a serem utilizados
          df (pd.DataFrame)*: Dataframe onde serão inserido os dados retornados
          index (int)*: Index da linha iterada
          row (List)*: Conteudo da linha iterada


    return: df(pd.DataFrame): Dataframe com a linha atualizada
    """
    for key, value in response.items():
        if key in row_list:
            row_value = row_list[key]
            if row_value["row"] not in df.columns:
                df.loc[:, row_value["row"]] = None
            if pd.isnull(row.get(row_value["row"])):
                if "filter" in row_value:
                    filter_code = row_value["filter"]
                    df.loc[index, row_value["row"]] = eval(filter_code)
                else:
                    df.loc[index, row_value["row"]] = value

    return df

# if __name__ == '__main__':
#     from dotenv import load_dotenv
#     from pipeline.extract import read_data
#     from pipeline.transform import concatenate_dataframes

#     # df = read_file(path= "data\input\All Time Worldwide Box Office (filter).csv", delimiter = ";", encoding = "latin1")
#     df = read_data(path='data\input')
#     df = concatenate_dataframes(df).head(10)
#     df = complete_df(df)
