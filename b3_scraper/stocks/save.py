from io import StringIO
from pathlib import Path

import pandas as pd
import psycopg2

KEEP_COLS = [
    "DATA",
    "CODNEG",
    "NOMRES",
    "ESPECI",
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
    "CODISI",
]


def read_data(file_name: str):
    compressed_path = Path("files/stocks/compressed")
    df = pd.read_parquet(
        compressed_path / file_name,
        engine="fastparquet",
        columns=KEEP_COLS,
    )
    return df


def chunks(df, chunk_size):
    """Yield successive chunk_size chunks from df."""
    for i in range(0, df.shape[0], chunk_size):
        yield df[i : i + chunk_size]


def create_table_if_not_exists(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_data (
        "DATA" BIGINT NOT NULL,
        "CODNEG" VARCHAR NOT NULL,
        "NOMRES" VARCHAR NOT NULL,
        "ESPECI" VARCHAR NOT NULL,
        "PREABE" INT NOT NULL,
        "PREMAX" INT NOT NULL,
        "PREMIN" INT NOT NULL,
        "PREMED" INT NOT NULL,
        "PREULT" INT NOT NULL,
        "PREOFC" INT NOT NULL,
        "PREOFV" INT NOT NULL,
        "TOTNEG" BIGINT NOT NULL,
        "QUATOT" BIGINT NOT NULL,
        "VOLTOT" BIGINT NOT NULL,
        "CODISI" VARCHAR NOT NULL
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()


def insert_data(df, conn):
    for chunk_df in chunks(df, chunk_size=1_000):
        try:
            with conn.cursor() as cur:
                buffer = StringIO()
                chunk_df.to_csv(buffer, sep=";", header=False, index=False)
                buffer.seek(0)
                cur.copy_from(buffer, "stock_data", sep=";", null="NULL")
            conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            raise e


def main():
    conn = psycopg2.connect(
        dbname="b3_scraper",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432",
    )

    create_table_if_not_exists(conn)

    df = read_data("data_20240302.parquet")
    insert_data(df, conn)

    conn.close()


if __name__ == "__main__":
    main()
