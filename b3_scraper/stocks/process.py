from datetime import datetime
from pathlib import Path

import pandas as pd

B3_COTAHIST_ARGS = {
    "wd": [
        2,
        8,
        2,
        12,
        3,
        12,
        10,
        3,
        4,
        13,
        13,
        13,
        13,
        13,
        13,
        13,
        5,
        18,
        18,
        13,
        1,
        8,
        7,
        13,
        12,
        3,
    ],
    "columns_b3": [
        "TIPREG",
        "DATA",
        "CODBDI",
        "CODNEG",
        "TPMERC",
        "NOMRES",
        "ESPECI",
        "PRAZOT",
        "MODREF",
        "PREABE",
        "PREMAX",
        "PREMIN",
        "PREMED",
        "PREULT",
        "PREOFC",
        "PREOFV",
        "TOTNEG",
        "QUATOT",
        "VOLTOT",
        "PREEXE",
        "INDOPC",
        "DATVEN",
        "FATCOT",
        "POTEXE",
        "CODISI",
        "DISMES",
    ],
    "dtypes_b3": {
        "TIPREG": "int64",
        "DATA": "int64",
        "CODBDI": "category",
        "CODNEG": "object",
        "TPMERC": "int64",
        "NOMRES": "object",
        "ESPECI": "category",
        "PRAZOT": "object",
        "MODREF": "object",
        "PREABE": "int64",
        "PREMAX": "int64",
        "PREMIN": "int64",
        "PREMED": "int64",
        "PREULT": "int64",
        "PREOFC": "int64",
        "PREOFV": "int64",
        "TOTNEG": "int64",
        "QUATOT": "int64",
        "VOLTOT": "int64",
        "PREEXE": "int64",
        "INDOPC": "int64",
        "DATVEN": "int64",
        "FATCOT": "int64",
        "POTEXE": "float64",
        "CODISI": "object",
        "DISMES": "int64",
    },
}


def read_file(file_path: Path) -> pd.DataFrame:
    df = pd.read_fwf(
        file_path,
        header=None,
        names=B3_COTAHIST_ARGS["columns_b3"],
        dtype=B3_COTAHIST_ARGS["dtypes_b3"],
        widths=B3_COTAHIST_ARGS["wd"],
        skiprows=1,
        skipfooter=1,
        encoding="ISO-8859-1",
        engine="pyarrow",
    )

    filter_condition = (
        df["ESPECI"].str.contains(
            "ON|PN|PNB|PNA",
            regex=True,
            case=False,
        )
    ) & (df["CODBDI"] == "02")

    df = df[filter_condition].reset_index(drop=True)
    return df


def save_data(df: pd.DataFrame, file_name: str) -> None:
    compressed_path = Path("files/stocks/compressed")
    compressed_path.mkdir(parents=True, exist_ok=True)
    file_name = Path(file_name).with_suffix(".parquet")

    df.to_parquet(compressed_path / file_name, compression="snappy")


if __name__ == "__main__":
    extracted_path = Path("files/stocks/extracted")
    dfs = []
    for file_path in extracted_path.iterdir():
        tmp_df = read_file(file_path)
        dfs.append(tmp_df)

    df = pd.concat(dfs)
    today = datetime.today().strftime("%Y%m%d")
    save_data(df, f"data_{today}")
