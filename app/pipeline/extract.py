import glob  # biblioteca para listar arquivos
import os  # biblioteca para manipular arquivos e pastas
from typing import List

import pandas as pd


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

    for file in all_files:
        data_frame_list.append(
            read_file(path=file, delimiter=';', encoding='latin1')
        )

    return data_frame_list


if __name__ == '__main__':
    data_frame_list = read_data(path='data/input')
    print(data_frame_list)
