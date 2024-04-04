import re
from typing import List, Dict

import pandas as pd
# from api.consult import complete_df
from tqdm import tqdm
from pipeline.load import load_csv

def concatenate_dataframes(dataframe_list: List[pd.DataFrame]) -> pd.DataFrame:
    """
    função para transformar uma lista de dataframes em um único dataframe

    args: list_dataframes (List): Lista de dataframes a serem concatenados

    return: dataframe
    """
    max_index: int = len(dataframe_list)

    dataframe_merge: pd.DataFrame = pd.DataFrame(columns=["Title Normalize"])
    for index, dataframe in tqdm(
        enumerate(dataframe_list), total=len(dataframe_list), desc="Criando DataFrame"
    ):
        dataframe["Title Normalize"] = dataframe["Title"].apply(normalize_tile_movie)
        dataframe_merge = pd.merge(
            dataframe_merge,
            dataframe,
            on="Title Normalize",
            how="outer",
            suffixes=("", f"_dup{index}"),
        )
    dataframe_merge = dataframe_merge.drop(columns=["Title Normalize"])
    dataframe_merge = remove_dup_columns(dataframe_merge, max_index)
    dataframe_merge["Year"] = dataframe_merge["Year"].apply(
        lambda row: int(row) if pd.notnull(row) else row
    )

    return dataframe_merge


def remove_dup_columns(df: pd.DataFrame, max_index: int) -> pd.DataFrame:
    """
    Remove qualquer coluna duplicada que possui o sufixo "_dup"

    args: df(pd.dataframe): Dataframe
          column_name(str): Nome da coluna original

    return: df(pd.dataframe): Dataframe sem as colunas (column_name) duplicadas.
    """
    for column in tqdm(
        df.columns, total=len(df.columns), desc=f"Removendo Colunas Duplicadas"
    ):
        if f"_dup" in column:
            pattern = re.compile(r"_dup\d*$")
            column_name: str = re.sub(pattern, "", str(column))
            df[column_name] = df.apply(
                lambda row: fill_column(row, column_name, max_index),
                axis=1,
            )
            df = df.drop(columns=[column])

    return df


def normalize_tile_movie(nome_filme):
    """
    Converter para minúsculas e remove caracteres especiais

    args: nome_filme (str): Nome do Filme

    return: nome_filme_normalizado (str): Nome do Filme Normalizado
    """
    nome_filme_normalizado = nome_filme.lower()
    nome_filme_normalizado = re.sub(r"[^a-z0-9]", "", nome_filme_normalizado)
    return nome_filme_normalizado


def fill_column(row: List, column: str, max_index: int):
    """
    Preenche os valores de uma coluna baseados nos valores de
    colunas com o mesmo nome.

    args: row (List): Linha a ser preenchida
          column (str): Nome da coluna
          max_index: Número maximo de colunas com o mesmo nome

    return: row[column]: Linha preenchida com o valor da coluna
    """
    if pd.notnull(row[column]):
        return row[column]

    for i in range(max_index + 1):
        column_name = f"{column}_dup{i}"
        if column_name in row and pd.notnull(row[column_name]):
            return row[column_name]
        
def fill_dataframe(
        df, 
        columns_to_split: Dict = {
            'Cast':{
                'number_columns': 3
            },
            'Director': {
                'number_columns': 2
            }, 
        }
    ):
    
    df = df[(df['WorldwideBox_office'] != 0) & (df['WorldwideBox_office'].notna()) & (df['DomesticBox_office'] != 0) & (df['DomesticBox_office'].notna())]
    df = df[(df['WorldwideBox_office'].notna()) & (df['Director'].notna())]
    df.loc[:, 'InternationalBox_office'] = df['WorldwideBox_office'] - df['DomesticBox_office']

    for column, config in columns_to_split.items():
        df.loc[:, column] = df[column].fillna('')
        number_columns = config['number_columns']
        df.loc[:, column] = df[column].apply(lambda x: x.split(',')[:number_columns])

        for i in range(number_columns):
            df.loc[:, f'{column}_{i+1}'] = df[column].apply(lambda x: x[i] if len(x) > i else None)        

    df = df.drop(columns=list(columns_to_split.keys()))

    return df


if __name__ == "__main__":
    from extract import read_data

    df = read_data(path="data/input")
    data_frame_list = concatenate_dataframes(df)
