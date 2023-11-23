# let's start again
import pandas as pd

data = pd.read_excel("2022_data/2022_consolidated/2022_BS.xlsx")
unique_values = data['항목명'].unique()
print(unique_values)