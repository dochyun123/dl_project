# let's start again
import pandas as pd

data = pd.read_excel("C:/Users/dochy/Desktop/고려대/딥러닝/project/fsdata/2023_1Q_BS.xlsx")
print(data.head())
print(data.describe())
print(data.iloc[0:10])
