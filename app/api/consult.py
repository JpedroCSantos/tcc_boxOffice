import os
from typing import Dict, List

import pandas as pd
from api.consult_omdb import get_movie
from api.consult_tmdb import search_details_movie, search_movie
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv(dotenv_path="env\.env")

def complete_df(df: pd.DataFrame) -> pd.DataFrame:
    for index, row in tqdm(df.iterrows(), total=len(df), desc='Completando o Dataframe:'):
        # Verificar se há algum valor NaN na linha atual
        if row.isnull().any():
            data_tmdb = search_movie(
                query = row['Title'], 
                api_key= os.getenv("TMDB_AUTORIZATION"),
                year = row['Year'],
            )
            if data_tmdb and data_tmdb[0]["id"]:
                response = search_details_movie(
                    movie_id = data_tmdb[0]["id"],
                    api_key=os.getenv("TMDB_AUTORIZATION")
                )
                ROWS_TMDB: Dict = {
                    'budget': {
                        'row': 'production_cost',
                    },
                    'runtime': {
                        'row': 'Runtime',
                    },
                    'revenue': {
                        'row': 'WorldwideBox_office',
                    },
                    # 'popularity': {
                    #     'row': 'popularity',
                    # },
                    # 'release_date': {
                    #     'row': 'release_date',
                    # }
                }
                for key, value in response.items():
                    if key in ROWS_TMDB and pd.isnull(row[ROWS_TMDB[key]['row']]):
                        df.at[index, ROWS_TMDB[key]['row']] = value
    return df

                
# if __name__ == '__main__':
#     from dotenv import load_dotenv
#     from pipeline.extract import read_data
#     from pipeline.transform import concatenate_dataframes

#     # df = read_file(path= "data\input\All Time Worldwide Box Office (filter).csv", delimiter = ";", encoding = "latin1")
#     df = read_data(path='data\input')
#     df = concatenate_dataframes(df).head(10)
#     df = complete_df(df)