from tools.df_manipulation import ReadyDF
import pandas as pd

df = pd.read_csv('../websites.csv')
df = df.normalize()
