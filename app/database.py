from api.consult import complete_df
from dotenv import load_dotenv
from pipeline.extract import read_data, read_file, exists_database
from pipeline.load import load_csv, load_json
from pipeline.transform import concatenate_dataframes

# df = read_file(path= "data\input\All Time Worldwide Box Office (filter).csv", delimiter = ";", encoding = "latin1")
FINAL_PATH = 'C:/Users/JPedr/OneDrive/Documentos/TCC/Projeto/data/output'

df, file_names = read_data(path='data\input')
if exists_database(path = FINAL_PATH, list_of_files = file_names):
    print("Exists database")
    df = read_file(path = f'{FINAL_PATH}/Box_Office DataBase.csv')
else:
    print("Update database")
    df = concatenate_dataframes(df)
    load_json(
        content=file_names,
        output_path= FINAL_PATH,
        filename='Box_Office DataBase',
    )

# df = complete_df(df)
print(df)
print(df.columns)

# load_csv(
#     data_frame=df,
#     output_path= FINAL_PATH,
#     filename='Box_Office DataBase',
# )