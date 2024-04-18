import os
import pandas as pd

from api.consult import complete_df
from api.consult_omdb import OMDBMovieAPI
from api.consult_tmdb import TMDBMovieAPI
from dotenv import load_dotenv
from pipeline.extract import (
    read_data,
    read_file,
    createOrUpdateDatabase
)
from pipeline.load import load_csv, load_json, transform_dataframe_to_db
from pipeline.transform import concatenate_dataframes, fill_dataframe

def build_apis_objects():
    apis = []
    apis.append(tmdb_api = TMDBMovieAPI(os.getenv("TMDB_KEY")))
    apis.append(omdb_api = OMDBMovieAPI(os.getenv("OMDB_KEY")))

    return apis

def filter_dataframe(df: pd.dataframe):
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

FINAL_PATH = "C:/Users/JPedr/OneDrive/Documentos/TCC/Projeto_2/data/output"
FILE_NAME_FIRST_DATABASE = "Box_Office DataBase(not_complete)"
INITIAL_PATH = "data/input"
COMPLETE_DATAFRAME = False
READ_FILE = True

load_dotenv(dotenv_path="env/.env")
if READ_FILE:
    FILE_TO_READ = "C:/Users/JPedr/OneDrive/Documentos/TCC/Projeto_2/data/input/Box_Office DataBase(filter).csv"
    df = read_file(path=FILE_TO_READ, delimiter= ";")
else:
    df_files, file_names = read_data(path= INITIAL_PATH)

updateOrCreate, df = createOrUpdateDatabase(
    path=FINAL_PATH, 
    list_of_files=file_names, 
    file_name= FILE_NAME_FIRST_DATABASE
)
updateOrCreate = False 
print(updateOrCreate),
print(df)

if updateOrCreate and df is not None:
    df = concatenate_dataframes(df)

elif updateOrCreate and df is None:
    df = concatenate_dataframes(df_files)

load_csv(
    data_frame=df,
    output_path=FINAL_PATH,
    filename= "Box_Office DataBase(without_filter)",
    delimiter= ";"
)

if COMPLETE_DATAFRAME:
    apis_to_consult = build_apis_objects()
    df = complete_df(df, apis_to_consult)
    print(df)
    print(df.columns)

df = filter_dataframe(df)

load_csv(
    data_frame=df,
    output_path=FINAL_PATH,
    filename="Box_Office DataBase(filter)",
    delimiter= ";"
)
load_json(
    content=file_names,
    output_path=FINAL_PATH,
    filename="Box_Office DataBase",
)

transform_dataframe_to_db(df)