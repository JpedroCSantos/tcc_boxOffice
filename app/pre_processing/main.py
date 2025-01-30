import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from transform import build_target_features, enconding_features, normalize_df
from process import create_model_linearRegression


PATH ="data\input\Box_Office DataBase(filter).csv"
df = pd.read_csv(PATH, delimiter=";", encoding="utf-8")
print(df)

df = df.drop('DomesticBox_office', axis=1)
df = df.drop('InternationalBox_office', axis=1)
df = df.drop('production_cost', axis=1)
df = df.drop('id', axis=1)
df = df.drop('Title', axis=1)
df = df.drop('Year', axis=1)
df = df.drop('vote_average', axis=1) # Removida por corela;'ao com a variavel IMDB
df = df.drop('vote_count', axis=1) # Removida por corela;'ao com a variavel IMDB

df = df.dropna()
target = df["WorldwideBox_office"]
df_features = build_target_features(df, "WorldwideBox_office")

df_features = enconding_features(['Cast_1','Cast_2','Cast_3'],df_features)
df_features = enconding_features(['Genre_1','Genre_2','Genre_3'],df_features)
df_features = enconding_features('Director_1',df_features)
df_features = enconding_features('original_language',df_features)
df_features = enconding_features('Production_Companies',df_features)

# df_features = normalize_df('StandardScaler', df_features)
df_features = normalize_df('MinMaxScaler', df_features)
# df_features = df_features.dropna()
print(df_features)
print(df_features.describe())
print(len(df_features))
print(len(target))


correlation_matrix = df_features.corr()
# plt.figure(figsize=(8, 6))
# sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1)
# plt.title('Matriz de Correlação')
# plt.show()

create_model_linearRegression(df_features, target)

df_features = df_features.drop('IMDB_Rating', axis=1) # Removida por corela;'ao com a variavel IMDB
create_model_linearRegression(df_features, target)