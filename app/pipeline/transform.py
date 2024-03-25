import re
from typing import List

import pandas as pd

# firstInteraction = True
# for dataframe in list_dataframes:
#     if firstInteraction:
#         firstInteraction = False
#         pass
#     else:
#        dataframe = dataframe.drop(0)


def concatenate_dataframes(dataframe_list: List[pd.DataFrame]) -> pd.DataFrame:
    """
    função para transformar uma lista de dataframes em um único dataframe

    args: list_dataframes (List): Lista de dataframes a serem concatenados

    return: dataframe
    """
    columns: List[str] = []
    max_index: int = len(dataframe_list)

    for dataframe in dataframe_list:
        columns.extend(dataframe.columns)
    columns = list(set(columns))

    dataframe_merge: pd.DataFrame = pd.DataFrame(columns=['Title Normalize'])
    for index, dataframe in enumerate(dataframe_list):
        dataframe['Title Normalize'] = dataframe['Title'].apply(
            normalize_tile_movie
        )
        dataframe_merge = pd.merge(
            dataframe_merge,
            dataframe,
            on='Title Normalize',
            how='outer',
            suffixes=('', f'_dup{index}'),
        )
    dataframe_merge = dataframe_merge.drop(columns=['Title Normalize'])

    for column in dataframe_merge.columns:
        if '_dup' in column:
            pattern = re.compile(r'_dup\d*$')
            column_name: str = re.sub(pattern, '', str(column))
            dataframe_merge[column_name] = dataframe_merge.apply(
                lambda row: fill_column(row, column_name, max_index),
                axis=1,
            )
            dataframe_merge = remove_dup_columns(dataframe_merge, column_name)

    return dataframe_merge


def remove_dup_columns(df: pd.DataFrame, column_name: str):
    """
    Remove qualquer coluna duplicada que possui o sufixo "_dup"

    args: df(pd.dataframe): Dataframe
          column_name(str): Nome da coluna original

    return: df(pd.dataframe): Dataframe sem as colunas (column_name) duplicadas.
    """
    for column in df.columns:
        if f'{column_name}_dup' in column:
            df = df.drop(columns=[column])
    return df


def normalize_tile_movie(nome_filme):
    """
    Converter para minúsculas e remove caracteres especiais

    args: nome_filme (str): Nome do Filme

    return: nome_filme_normalizado (str): Nome do Filme Normalizado
    """
    nome_filme_normalizado = nome_filme.lower()
    nome_filme_normalizado = re.sub(r'[^a-z0-9\s]', '', nome_filme_normalizado)
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
        column_name = f'{column}_dup{i}'
        if column_name in row and pd.notnull(row[column_name]):
            return row[column_name]


if __name__ == '__main__':
    from extract import read_data

    df = read_data(path='data\input')
    data_frame_list = concatenate_dataframes(df)
