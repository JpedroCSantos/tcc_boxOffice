import re
from typing import Dict, List

import pandas as pd
from pipeline.load import load_csv

# from api.consult import complete_df
from tqdm import tqdm


def concatenate_dataframes(dataframe_list: List[pd.DataFrame]) -> pd.DataFrame:
    """
    função para transformar uma lista de dataframes em um único dataframe

    args: list_dataframes (List): Lista de dataframes a serem concatenados

    return: dataframe
    """
    max_index: int = len(dataframe_list)

    dataframe_merge: pd.DataFrame = pd.DataFrame(columns=["id"])
    for index, dataframe in tqdm(
        enumerate(dataframe_list), total=len(dataframe_list), desc="Criando DataFrame"
    ):
        dataframe_merge = pd.merge(
            dataframe_merge,
            dataframe,
            on="id",
            how="outer",
            suffixes=("", f"_dup{index}"),
        )
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
    columns_to_split: Dict,
    columns_to_fill_NaN_Values: List,
    columns_to_filter: Dict
):
    """
    Filtra o dataframe de acordo com as informações passadas.

    args: df(pd.dataframe): Dataframe a ser filtrado
        columns_to_split (Dict): Colunas a serem separadas
                            EX : {"Cast": {"number_columns": 3}}
        columns_to_fill_NaN_Values (List): Colunas a serem excluidos valores NaN
        columns_to_filter (Dict): Colunas a serem Filtradas
                             EX :"Metascore":{IMDB_Rating: {"filter" : "x: x / 10 if x > 100 else x"}}

    return: df(pd.dataframe): Dataframe filtrado
    """
    df = _fill_inconsistents_box_offices(df)
    df = _fill_NaN_rows(df, columns_to_fill_NaN_Values)
    df.loc[:, 'InternationalBox_office'] = df.apply(_calculate_InternationalBox_office, axis=1)
    df = _split_Columns(df, columns_to_split)
    df = _filter_columns(df, columns_to_filter)

    return df

def _filter_columns(df, columns_to_filter):
    # df.loc[:, "IMDB_Rating"] = df["IMDB_Rating"].apply(
    #     lambda x: x / 10 if x > 10 else x
    # )
    # df.loc[:, "Metascore"] = df["Metascore"].apply(lambda x: x / 10 if x > 100 else x)
    for column, filter in df.columns.items():
        df.loc[:, column] = df[column].apply(lambda x: eval(filter))

def _split_Columns(df, columns_to_split):
    for column, config in columns_to_split.items():
        df.loc[:, column] = df[column].fillna("")
        number_columns = config["number_columns"]
        df.loc[:, column] = df[column].apply(lambda x: x.split(",")[:number_columns])

        for i in range(number_columns):
            df.loc[:, f"{column}_{i+1}"] = df[column].apply(
                lambda x: x[i] if len(x) > i else None
        )
    df = df.drop(columns=list(columns_to_split.keys()))
    return df

def _fill_NaN_rows(df, columns):
    df = df.replace(['N/A', 'nan', None, '<NA>'], pd.NA)
    for column in columns:
        df = df[(df[column] != 0)
                &  (df[column].notna())]
    return df

def _fill_inconsistents_box_offices(df):
    inconsistent_box_office = df.loc[df['DomesticBox_office'] > df['WorldwideBox_office']]["id"]
    return df[~df['id'].isin(inconsistent_box_office)]

def _calculate_InternationalBox_office(row):
    if row['DomesticBox_office'] > row['WorldwideBox_office']:
        row['DomesticBox_office'], row['WorldwideBox_office'] = row['WorldwideBox_office'], row['DomesticBox_office']
        
    return row['WorldwideBox_office'] - row['DomesticBox_office']

def filter_dataframe(df: pd.DataFrame):
    return fill_dataframe(
        df = df, 
        columns_to_split = {
            "Cast": {"number_columns": 3},
            "Director": {"number_columns": 1},
        },
        columns_to_fill_NaN_Values = [
            "WorldwideBox_office",
            "DomesticBox_office"
        ],
        columns_to_filter = {
            "IMDB_Rating":{
                "filter" : "x: x / 10 if x > 10 else x"
            },
            "Metascore":{
                "filter" : "x: x / 10 if x > 100 else x"
            },
        }
    )

if __name__ == "__main__":
    from extract import read_data

    df = read_data(path="data/input")
    data_frame_list = concatenate_dataframes(df)
