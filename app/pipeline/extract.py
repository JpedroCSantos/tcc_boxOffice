import glob  # biblioteca para listar arquivos
import os  # biblioteca para manipular arquivos e pastas
from typing import List

import pandas as pd
import json


def read_file(
    path: str, delimiter: str = ',', encoding: str = 'utf-8'
) -> pd.DataFrame:
    """
    function para ler um arquivo .csv"" e retornar uma dataframe

    args: path (srt): caminho arquivos
          file (file.csv): arquivo
          delimiter(srt): Delimitador do arquivo csv (Default: ',')
          encoding(srt): Encode do arquivo csv (Default: 'utf-8')

    return: dataframe
    """

    try:
        # Lendo o arquivo CSV em um DataFrame pandas
        df = pd.read_csv(path, delimiter=delimiter, encoding=encoding)
        return df
    except FileNotFoundError:
        print('Arquivo não encontrado:', path)
        return None
    except Exception as e:
        print('Ocorreu um erro ao ler o arquivo:', e)
        return None


def read_data(path: str) -> List[pd.DataFrame]:
    """
    function para ler arquivos .csv"" de uma pasta data/input
    e retornar uma lista de dataframes

    args: path (srt): caminho da pasta com os arquivos

    return: lista de dataframe
    """

    all_files = glob.glob(os.path.join(path, '*.csv'))

    data_frame_list = []
    file_names = [os.path.basename(arquivo) for arquivo in all_files]

    for file in all_files:
        data_frame_list.append(
            read_file(path=file, delimiter=';', encoding='latin1')
        )

    return data_frame_list, file_names

def exists_database(path: str, list_of_files: List[str]) -> bool:
    print(list_of_files)
    """
    function para verificar se já existe um arquivo de database
    e se os arquivos que formaram o mesmo são os que estão sendo
    carregados no momento. Retorna um boolean caso exista e os arquvos
    geradores são os mesmos.

    args: path (srt): caminho da pasta com o arquivo final
          list_of_files(List[str]): Arquivos para formar a database

    return: bool
    """
    # print(f'Existe o Arquivo: {os.path.exists(f'{path}/Box_Office DataBase.csv')}')
    # print(f'Existe o Arquivo JSON: {os.path.exists(f'{path}/Box_Office DataBase.json')}')
    # print(f'Mesmos Arquivos: {_same_creator_files(path, list_of_files)}')
    if ((os.path.exists(f'{path}/Box_Office DataBase.csv'))
        and  (os.path.exists(f'{path}/Box_Office DataBase.json'))
        and _same_creator_files(path, list_of_files)):
        return True
    else:
        return False
    
def _same_creator_files(path: str, list_of_files: List[str]):
    with open(f'{path}/Box_Office DataBase.json', 'r') as file:
        list_creator_files = list(json.load(file).keys())
        if [chave for chave in list_creator_files if chave not in list_of_files] or [key for key in list_of_files if key not in list_creator_files]:
            return False
        else:
            return True

if __name__ == '__main__':
    data_frame_list, file_names = read_data(path='data/input')
    print(file_names)
    # print(data_frame_list)
