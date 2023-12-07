import pandas as pd

# 1step : preprocess

BS_item = ["유동자산", "비유동자산", "자산총계", "자본총계", "부채및자본총계", "유동부채", "비유동부채", "부채총계"]
CF_item = ["영업활동현금흐름"]
PL_item = ["매출액", "매출총이익", "순이익"]


def select_non(series):
    return series.dropna().iloc[0] if not series.dropna().empty else None


def preprocess(file_path, item_names_to_extract):
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
    grouped_data = (
        extracted_data.groupby(["회사명", "종목코드", "항목명"])
        .agg({"당기": select_non})
        .reset_index()
    )

    final_data = grouped_data.pivot(
        index=["회사명", "종목코드"], columns="항목명", values="당기"
    ).reset_index()

    return final_data


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


filterd_BS_1Q_2020 = preprocess("2020_1Q_BS_filter.xlsx", BS_item)
filterd_CF_1Q_2020 = preprocess("2020_1Q_CF_filter.xlsx", CF_item)
filterd_PL_1Q_2020 = preprocess("2020_1Q_PL_filter.xlsx", PL_item)

filterd_BS_1Q_2020 = fillnan(filterd_BS_1Q_2020)
filterd_CF_1Q_2020 = fillnan(filterd_CF_1Q_2020)
filterd_PL_1Q_2020 = fillnan(filterd_PL_1Q_2020)

df_1Q_2020 = merge(filterd_BS_1Q_2020, filterd_CF_1Q_2020, filterd_PL_1Q_2020)
df_1Q_2020.to_csv("df_1Q_2020")


filterd_BS_1Q_2023 = preprocess("2023_1Q_BS_filter.xlsx", BS_item)
filterd_CF_1Q_2023 = preprocess("2023_1Q_CF_filter.xlsx", CF_item)
filterd_PL_1Q_2023 = preprocess("2023_1Q_PL_filter.xlsx", PL_item)

filterd_BS_1Q_2023 = fillnan(filterd_BS_1Q_2023)
filterd_CF_1Q_2023 = fillnan(filterd_CF_1Q_2023)
filterd_PL_1Q_2023 = fillnan(filterd_PL_1Q_2023)

df_1Q_2020 = merge(filterd_BS_1Q_2023, filterd_CF_1Q_2023, filterd_PL_1Q_2023)
df_1Q_2020.to_csv("df_1Q_2023")


def calculate_financial_ratios(df):
    # Calculating each ratio
    df["Current Ratio"] = df["유동자산"] / df["유동부채"]
    df["Quick Ratio"] = (df["유동자산"] - df["재고자산"]) / df["유동부채"]
    df["Debt Ratio"] = df["부채 총계"] / df["자본총계"]
    df["Fixed Asset Turnover"] = df["매출액"] / df["비유동자산"]
    df["Equity Turnover"] = df["매출액"] / df["자산총계"]
    df["ROE"] = df["순이익"] / df["자본총계"]
    df["ROA"] = df["순이익"] / df["자산총계"]
    df["Gross Margin Ratio"] = df["매출 총이익"] / df["매출액"]
    df["Operating Cash Flow Ratio"] = df["영업활동현금흐름"] / df["유동부채"]
    df["Cash Flow Coverage Ratio"] = df["영업활동현금흐름"] / df["부채 총계"]
    df["Cash Flow Margin Ratio"] = df["영업활동현금흐름"] / df["순이익"]
    return df
