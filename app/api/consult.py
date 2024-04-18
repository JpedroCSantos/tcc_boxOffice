import re
from datetime import datetime
from typing import Dict, List

import pandas as pd
# from classes.consult_class import MovieAPI
from api.classes.consult_class import MovieAPI
from tqdm import tqdm


def complete_df(df: pd.DataFrame, apis: List[MovieAPI]) -> pd.DataFrame:
    """
    Função para completar as informações vazias do dataframe,
    através de consultas a várias APIs.

    Args:
        df (pd.DataFrame): dataframe a ser completado
        apis (List[MovieAPI]): lista de instâncias de classes que implementam a interface MovieAPI para fazer as consultas à API

    Returns:
        df(pd.DataFrame): dataframe completo
    """
    all_rows = []
    all_rows = [api.get_api_dict() for api in apis]
    ROWS = {}
    for row in all_rows:
        ROWS.update(row)

    data_cache = {index: {} for index in df.index}
    rows_to_update = []

    # Iteração sobre o dataframe para verificar quais linhas precisam ser atualizadas
    for index, row in tqdm(df.iterrows(), total=len(df), desc="Consultando API"):
        columns_to_update = [
            value["row"]
            for key, value in ROWS.items()
            if pd.isna(pd.Series(row.get(value["row"]))).all()
        ]
        if columns_to_update:
            rows_to_update.append(index)

        for api in apis:
            if any(columns_to_update) and not data_cache[index].get(api):
                data_cache[index][api] = api.search_movie(row, data_cache[index])

    for index in tqdm(
        rows_to_update, total=len(rows_to_update), desc="Atualizando Dataframe"
    ):
        row = df.loc[index]
        for api in apis:
            if data_cache[index].get(api) is not None:
                df = insert_info_movie(data_cache[index][api], ROWS, df, index, row)

    return df


def insert_info_movie(
    response: Dict, row_list: Dict, df: pd.DataFrame, index: int, row: List
) -> pd.DataFrame:
    """
    Função para inserir os dados retornados da api no dataframe

    args: response(Dict)*: Retorno da API
          row_list (Dict[str])*: Dicionario com os parâmetros de rotorno a serem utilizados
          df (pd.DataFrame)*: Dataframe onde serão inserido os dados retornados
          index (int)*: Index da linha iterada
          row (List)*: Conteudo da linha iterada


    return: df(pd.DataFrame): Dataframe com a linha atualizada
    """
    for key, value in dict(response).items():
        if key in row_list and value is not None:
            row_value = row_list[key]
            if row_value["row"] not in df.columns:
                df.loc[:, row_value["row"]] = None
            df.loc[index, row_value["row"]] = value
    return df


if __name__ == "__main__":
    import os
    import sys

    from consult_omdb import OMDBMovieAPI
    from consult_tmdb import TMDBMovieAPI
    from dotenv import load_dotenv

    # Adicione o diretório raiz do projeto ao PYTHONPATH
    sys.path.append(
        os.path.dirname(
            os.path.dirname(
                "C:/Users/JPedr/OneDrive/Documentos/TCC/Projeto/app/pipeline/transform.py"
            )
        )
    )
    sys.path.append(
        os.path.dirname(
            os.path.dirname(
                "C:/Users/JPedr/OneDrive/Documentos/TCC/Projeto/app/pipeline/extract.py"
            )
        )
    )

    from pipeline.extract import read_data
    from pipeline.transform import concatenate_dataframes

    load_dotenv(dotenv_path="env/.env")

    tmdb_api = TMDBMovieAPI(os.getenv("TMDB_KEY"))
    omdb_api = OMDBMovieAPI(os.getenv("OMDB_KEY"))
    df, file_names = read_data(path="data/input")

    df = concatenate_dataframes(df).head(10)
    print(df)

    df = complete_df(df=df, apis=[tmdb_api, omdb_api])
    print(df)
