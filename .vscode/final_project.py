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


import pandas as pd

# 1step : preprocess

BS_item = ["유동자산", "비유동자산", "자산총계", "자본총계", "부채및자본총계", "유동부채", "비유동부채", "부채총계"]
CF_item = ["영업활동현금흐름"]
IC_item = ["매출액", "매출총이익", "순이익"]


def preprocess(
    file_path, item_names_to_extract
):  # item_names 에 BS_item / CF_item / IC_item 넣기
    # Load the data
    data = pd.read_excel(file_path)

    # Erase blank spaces and special characters in '항목명'
    data["항목명"] = (
        data["항목명"].str.replace(" ", "").str.replace(r"[^가-힣]", "", regex=True)
    )

    # If '종목코드' column is present, format it by removing brackets
    if "종목코드" in data.columns:
        data["종목코드"] = data["종목코드"].str.replace("[", "").str.replace("]", "")

    # Extract the specified item names
    extracted_data = data[data["항목명"].isin(item_names_to_extract)]
    extracted_data = extracted_data.pivot(
        index=["회사명", "종목코드"], columns="항목명", values="당기 1분기"
    ).reset_index()

    return extracted_data


# 2nd step : fillna preprocessed dataframe


def fillnan(df):
    # Filling NaN values using the provided formulas
    df["자산총계"] = df["자산총계"].fillna(df["유동자산"] + df["비유동자산"])
    df["부채총계"] = df["부채총계"].fillna(df["유동부채"] + df["비유동부채"])
    df["부채및자본총계"] = df["부채및자본총계"].fillna(df["부채총계"] + df["자본총계"])
    df["자본총계"] = df["자본총계"].fillna(df["부채및자본총계"] - df["부채총계"])
    return df


# 3rd step : merge preprocessed dataframe

import pandas as pd


def merge(BS_df, CF_df, IC_df):
    merged_df = pd.merge(BS_df, CF_df, on=["회사명", "종목코드"], how="outer")
    merged_df = pd.merge(merged_df, IC_df, on=["회사명", "종목코드"], how="outer")

    return merged_df


# 4th step : merge label
def create_class_and_merge(data_df, label_df, drop_columns):
    label_df["class"] = label_df["등락률"].apply(
        lambda x: 0 if x <= -10 else (1 if x < 0 else (2 if x < 10 else 3))
    )
    label_df = label_df.drop(drop_columns, axis=1)
    merged_df = pd.merge(data_df, label_df, on=["종목코드"], how="outer")

    return merged_df
