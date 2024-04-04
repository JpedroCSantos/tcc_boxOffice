from dotenv import load_dotenv
from pipeline.extract import exists_database, read_data, read_file, same_creator_files, new_files
from pipeline.load import load_csv, load_json
from pipeline.transform import concatenate_dataframes, fill_dataframe
from api.consult import complete_df
from api.consult_tmdb import TMDBMovieAPI
from api.consult_omdb import OMDBMovieAPI

import os

load_dotenv(dotenv_path="env/.env")
FINAL_PATH = "C:/Users/JPedr/OneDrive/Documentos/TCC/Projeto/data/output"
df, file_names = read_data(path="data/input")
tmdb_api = TMDBMovieAPI(os.getenv("TMDB_KEY"))
omdb_api = OMDBMovieAPI(os.getenv("OMDB_KEY"))

if exists_database(path=FINAL_PATH, list_of_files=file_names):
    if same_creator_files(path=FINAL_PATH, list_of_files=file_names):
        print("Update database")
        df = read_file(path=f"{FINAL_PATH}/Box_Office DataBase(not_filtered).csv")

    else:    
        print("Merge and update database")
        df_list = []
        df_list.append(read_file(path=f"{FINAL_PATH}/Box_Office DataBase(not_filtered).csv"))
        files_to_merge = new_files(input_path = "data/input", output_path= FINAL_PATH)
        for file in files_to_merge:
            df_list.append(read_file(path=f"data/input/{file}", delimiter=";", encoding="latin1"))
        df = concatenate_dataframes(df_list)

else:
    print("Create database")
    df = concatenate_dataframes(df)

df = complete_df(df, [tmdb_api, omdb_api])
print(df)
print(df.columns)

load_csv(
    data_frame=df,
    output_path=FINAL_PATH,
    filename="Box_Office DataBase(not_filtered)",
)

df = fill_dataframe(df)
print(df)
print(df.columns)

load_csv(
    data_frame=df,
    output_path=FINAL_PATH,
    filename="Box_Office DataBase",
)
load_json(
    content=file_names,
    output_path=FINAL_PATH,
    filename="Box_Office DataBase",
)
