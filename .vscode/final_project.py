# let's start again
import pandas as pd

data = pd.read_excel("2022_data/2022_consolidated/2022_BS.xlsx")
unique_values = data['항목명'].unique()
print(unique_values)


def update_df_with_item(df_source, df_target, item_name, target_column_name):
    for index, row in df_target.iterrows():
        # Find matching value in df_source
        value = df_source[(df_source['회사명'] == row['회사명']) & (df_source['항목명'] == item_name)]['당기']
        
        # Assign the value to the target column in df_target
        if not value.empty:
            df_target.at[index, target_column_name] = value.iloc[0]