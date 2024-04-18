import glob  # biblioteca para listar arquivos
import json
import os  # biblioteca para manipular arquivos e pastas
from typing import List

import pandas as pd


def read_file(path: str, delimiter: str = ",", encoding: str = "utf-8") -> pd.DataFrame:
    """
    function para ler um arquivo .csv"" e retornar uma dataframe

    args: path (srt): caminho arquivo
          delimiter(srt): Delimitador do arquivo csv (Default: ',')
          encoding(srt): Encode do arquivo csv (Default: 'utf-8')

    return: dataframe
    """

    try:
        # Lendo o arquivo CSV em um DataFrame pandas
        df = pd.read_csv(path, delimiter=delimiter, encoding=encoding)
        return df
    except FileNotFoundError:
        print("Arquivo não encontrado:", path)
        return None
    except Exception as e:
        print("Ocorreu um erro ao ler o arquivo:", e)
        return None


def read_data(path: str) -> List[pd.DataFrame]:
    """
    function para ler arquivos .csv"" de uma pasta data/input
    e retornar uma lista de dataframes

    args: path (srt): caminho da pasta com os arquivos

    return: lista de dataframe
    """

    all_files = glob.glob(os.path.join(path, "*.csv"))

    data_frame_list = []
    file_names = [os.path.basename(arquivo) for arquivo in all_files]

    for file in all_files:
        data_frame_list.append(read_file(path=file, delimiter=";"))

    return data_frame_list, file_names

def createOrUpdateDatabase(path: str, list_of_files: List[str], file_name = str) -> bool:
    """
    function para verificar se já existe um arquivo de database
    e se os arquivos que formaram o mesmo são os que estão sendo
    carregados no momento. Retorna um boolean caso exista e os arquvos
    geradores são os mesmos.

    args: path (srt): caminho da pasta com o arquivo final
          list_of_files(List[str]): Arquivos para formar a database

    return: bool
    """
    updateOrCreate = False
    df = None

    if exists_database(path=path):
        if _same_creator_files(path, list_of_files):
            print("Update database")
            df = read_file(path=f"{path}/{file_name}", delimiter= ";")
            return updateOrCreate, df
        
        else:
            print("Merge and update database")
            df_list = []
            df_list.append(
                read_file(path=f"{path}/{file_name}")
            )
            files_to_merge = _new_files(input_path="data/input", output_path=path)
            for file in files_to_merge:
                df_list.append(
                    read_file(path=f"data/input/{file}", delimiter=";")
                )
            updateOrCreate = True
            return updateOrCreate, df_list
    else:
        print("Create database")
        updateOrCreate = True
        return updateOrCreate, df

def exists_database(path: str) -> bool:
    """
    function para verificar se já existe um arquivo de database
    e se os arquivos que formaram o mesmo são os que estão sendo
    carregados no momento. Retorna um boolean caso exista e os arquvos
    geradores são os mesmos.

    args: path (srt): caminho da pasta com o arquivo final
          list_of_files(List[str]): Arquivos para formar a database

    return: bool
    """
    if (os.path.exists(f"{path}/Box_Office DataBase(not_filtered).csv")) and (
        os.path.exists(f"{path}/Box_Office DataBase.json")
    ):
        return True
    else:
        return False

def _same_creator_files(path: str, list_of_files: List[str]):
    with open(f"{path}/Box_Office DataBase.json", "r") as file:
        list_creator_files = list(json.load(file).keys())
        if [chave for chave in list_creator_files if chave not in list_of_files] or [
            key for key in list_of_files if key not in list_creator_files
        ]:
            return False
        else:
            return True


def _new_files(input_path: str, output_path: str):
    with open(f"{output_path}/Box_Office DataBase.json", "r") as file:
        list_creator_files = list(json.load(file).keys())

    all_files = glob.glob(os.path.join(input_path, "*.csv"))
    file_names = [os.path.basename(arquivo) for arquivo in all_files]
    new_files = list(set(file_names).symmetric_difference(set(list_creator_files)))
    return new_files


if __name__ == "__main__":
    df, file_names = read_data(path="data/input")
    # data_frame = read_data(path="data/output/Box_Office DataBase.csv", delimiter=",")
    print(file_names)
    # print(data_frame_list)
