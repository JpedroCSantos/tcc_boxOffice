from dotenv import load_dotenv
from pipeline.extract import exists_database, read_data, read_file, same_creator_files, new_files
from pipeline.load import load_csv, load_json
from pipeline.transform import concatenate_dataframes
from api.consult import complete_df

FINAL_PATH = "C:/Users/JPedr/OneDrive/Documentos/TCC/Projeto/data/output"
df, file_names = read_data(path="data/input")

if exists_database(path=FINAL_PATH, list_of_files=file_names):
    if same_creator_files(path=FINAL_PATH, list_of_files=file_names):
        print("Update database")
        df = read_file(path=f"{FINAL_PATH}/Box_Office DataBase.csv")
        df = complete_df(df)
    else:    
        print("Merge and update database")
        df_list = []
        df_list.append(read_file(path=f"{FINAL_PATH}/Box_Office DataBase.csv"))
        files_to_merge = new_files(input_path = "data/input", output_path= FINAL_PATH)
        for file in files_to_merge:
            df_list.append(read_file(path=f"data/input/{file}", delimiter=";", encoding="latin1"))
        print(df_list)
        df = concatenate_dataframes(df_list)
else:
    print("Create database")
    df = concatenate_dataframes(df)

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
