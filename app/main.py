from dotenv import load_dotenv
from pipeline.extract import read_data, read_file
from pipeline.load import load_csv
from pipeline.transform import concatenate_dataframes

# df = read_file(path= "data\input\All Time Worldwide Box Office (filter).csv", delimiter = ";", encoding = "latin1")
df = read_data(path='data\input')
df = concatenate_dataframes(df)
load_csv(
    data_frame=df,
    output_path='C:/Users/JPedr/OneDrive/Documentos/TCC/Projeto/data/output',
    filename='Box_Office DataBase',
)

load_dotenv(dotenv_path='env\.env.prod')
# load_dotenv()

# print(df)
