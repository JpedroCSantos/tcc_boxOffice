import os

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

    data_frame.to_csv(f'{output_path}/{filename}.csv', index=False)
    return 'Arquivo criado com sucesso'
