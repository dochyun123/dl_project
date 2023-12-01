# let's start again
import pandas as pd

data = pd.read_excel(
    "C:/Users/dochy/Desktop/고려대/딥러닝/project/fsdata/2023_1Q_BS_filter.xlsx"
)
unique_values = data["항목명"].unique()
print(unique_values)

import pandas as pd


class DataFrameProcessor:
    def __init__(
        self, source_df, target_df
    ):  # source : original data # target : new dataframe
        self.source_df = source_df
        self.target_df = target_df

    def clean_column(self, column_name):  # column_name '항목명'
        non_korean_regex = r"[^\uAC00-\uD7AF]"
        pattern = r"[^\uAC00-\uD7AF]|[\s]"
        self.source_df[column_name] = self.source_df[column_name].str.replace(
            non_korean_regex, "", regex=True
        )
        self.source_df[column_name] = self.source_df[column_name].str.replace(
            pattern, "", regex=True
        )

    def update_with_item(self, item_name, target_column_name, period):
        # period : 당기 or 전기 등
        for index, row in self.target_df.iterrows():
            value = self.source_df[
                (self.source_df["회사명"] == row["회사명"])
                & (self.source_df["항목명"] == item_name)
            ][period]
            if not value.empty:
                self.target_df.at[index, target_column_name] = value.iloc[0]

    def fill_missing_values(self, column_to_fill, operation):
        replace_value = operation(self.target_df)
        self.target_df[column_to_fill] = self.target_df[column_to_fill].fillna(
            replace_value
        )

    def get_modified_target_df(self):
        return self.target_df


# Initialize the processor with your dataframes
source_df = data
columns = [
    "회사명",
    "당기_유동자산",
    "당기_비유동자산",
    "당기_자본",
    "당기_유동부채",
    "당기_비유동부채",
    "당기_부채 및 자본총계",
    "당기_부채총계",
    "당기_자산총계",
]
target_df = pd.DataFrame(columns=columns)
processor = DataFrameProcessor(source_df, target_df)
print("Source DataFrame columns:", processor.source_df.columns)
print("Target DataFrame columns:", processor.target_df.columns)

# Clean the '항목명' column
processor.clean_column("항목명")

# Update df_2022_BS with various items
items_to_update = [
    ("유동자산", "당기_유동자산"),
    ("비유동자산", "당기_비유동자산"),
    ("유동부채", "당기_유동부채"),
    ("비유동부채", "당기_비유동부채"),
    ("자본총계", "당기_자본"),
    ("부채및자본총계", "당기_부채 및 자본총계"),
    ("부채총계", "당기_부채총계"),
    ("자산총계", "당기_자산총계"),
]

for item_name, target_column_name in items_to_update:
    processor.update_with_item(item_name, target_column_name, "당기 1분기말")

# Fill missing values
operation2 = lambda target_df: target_df["당기_부채총계"] - target_df["당기_유동부채"]
processor.fill_missing_values("당기_비유동부채", operation2)

operation3 = lambda target_df: target_df["당기_부채총계"] - target_df["당기_비유동부채"]
processor.fill_missing_values("당기_유동부채", operation3)

print(target_df)
print("finish")


def merge_with_label(data, label):
    # Clean the '종목코드' column in 'data' DataFrame
    data["종목코드"] = data["종목코드"].str.replace("[", "").str.replace("]", "")

    # Merge with the label DataFrame
    merged_df = pd.merge(data, label, on="종목코드", how="inner")

    return merged_df
