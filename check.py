import requests, pandas as pd
from dotenv import load_dotenv
import os
load_dotenv()
DART_KEY = os.getenv("DART_API_KEY")

r = requests.get(
    "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json",
    params={
        "crtfc_key": DART_KEY,
        "corp_code": "00139719",
        "bsns_year": "2024",
        "reprt_code": "11011",
        "fs_div": "CFS",
    }
)
df = pd.DataFrame(r.json()["list"])
print(df["sj_div"].unique())
print(df["sj_nm"].unique())