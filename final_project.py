KEY = "e7226c40eff83f44fb912ecf9f21ab39baf8e3d8"  # DART API

import dart_fss as dart
import pandas as pd

# Open DART API KEY 설정
api_key = KEY
dart.set_api_key(api_key=api_key)


corp_list = dart.get_corp_list()  # all corps : 103463 corps in list
start_date = "201200101"
end_date = "20201231"


def extract_fs(index, begin):
    corp_list = dart.get_corp_list()  # corp_name, corp_name None
    corp_name = corp_list[index].corp_name
    corporation = corp_list.find_by_corp_name(corp_name)[0]
    origin = corporation.extract_fs(
        bgn_de=begin,
        end_de=None,
        fs_tp=("bs", "is", "cis", "cf"),
        separate=False,
        report_tp="annual",
        lang="ko",
        separator=True,
        dataset="xbrl",
        cumulative=False,
        progressbar=True,
        skip_error=True,
        last_report_only=True,
    )
    bs_fs = origin["bs"]
    is_fs = origin["is"]
    cis_fs = origin["cis"]
    cf_fs = origin["cf"]
    return bs_fs, is_fs, cis_fs, cf_fs


for idx, corp in enumerate(corp_list):
    try:
        # Extract financial statements
        bs, is_, cis, cf = extract_fs(idx, start_date)

        # Concatenate the extracted data to the respective dataframes
        df_bs = pd.concat([df_bs, bs], ignore_index=True)
        df_is = pd.concat([df_is, is_], ignore_index=True)
        df_cis = pd.concat([df_cis, cis], ignore_index=True)
        df_cf = pd.concat([df_cf, cf], ignore_index=True)

    except Exception as e:
        print(f"Failed to extract data for {corp.corp_name}: {e}")
