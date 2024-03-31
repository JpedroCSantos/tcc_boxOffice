import json
import os
from typing import List

import pandas as pd


def load_csv(data_frame: pd.DataFrame, output_path: str, filename: str) -> str:
    """
    Recebe um dataframe e transforma em um arquivo csv

    args:
        data_frame (pd.dataframe): dataframe a ser convertido em excel
        output_path (str): caminho onde será salvo o arquivo
        filename (str): nome do arquivo a ser salvo

    return: "Arquivo salvo com sucesso"
    """
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    if os.path.exists(f"{output_path}/{filename}.csv"):
        os.remove(f"{output_path}/{filename}.csv")

    data_frame.to_csv(f"{output_path}/{filename}.csv", index=False)
    return "Arquivo CSV criado com sucesso"


def load_json(content: List[str], output_path: str, filename: str) -> str:
    """
    Recebe uma Lista de nome de arquivos e transforma em um arquivo json

    args:
        content (List[str]): Lista de nome de arquivos a ser convertido em JSON
        output_path (str): caminho onde será salvo o arquivo
        filename (str): nome do arquivo a ser salvo

    return: "Arquivo salvo com sucesso"
    """
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    if os.path.exists(f"{output_path}/{filename}.json"):
        os.remove(f"{output_path}/{filename}.json")

    data = {}
    for string in content:
        data[string] = True  # Definir os valores como None

    with open(f"{output_path}/{filename}.json", "w") as arquivo:
        json.dump(data, arquivo, indent=4)

    return "Arquivo JSON criado com sucesso"
