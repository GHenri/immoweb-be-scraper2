import time
from immoweb_scraper import automated_scraping
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from config import config as cfg
from argparse import ArgumentParser


def create_con(host, pw, user):
    # url = URL(drivername='postgresql', username=user, password=pw, host=host,
    #           database="real_estate", query={}, port=5432)
    url = URL.create(drivername="postgresql", username=user, host=host, database="real_estate", password=pw)
    engine = create_engine(url, future=True)

    sql = f"""   SELECT * 
                FROM {cfg["table"]}"""
    df_serv = pd.read_sql(text(sql), con=engine.connect())
    # df_serv = pd.read_csv("3270_3271_dup.csv")  # Enable for off-line tests

    return engine.connect(), df_serv


def update_existing_ids(df_serv: pd.DataFrame, df_scrap: pd.DataFrame, con: sqlalchemy.engine.Connection):
    # update records with same immoweb_id
    df_scrap = df_scrap[df_scrap['immoweb_id'].isin(df_serv['immoweb_id'].tolist())]
    df_merge = df_scrap.merge(df_serv, how='inner', on=['immoweb_id'])
    df_merge = df_merge.dropna()
    # Only update if sale price is updated
    df_updated = df_merge[(df_merge['transSalePrice_x'] != df_merge['transSalePrice_y'])]
    i = 0
    for id_ in df_updated['id']:
        sql = f"""INSERT INTO {cfg["backup"]} ("immoweb_id", "customerName", "propType", "propStreet", 
                                            "propHouseNo", "propLandSurface", "transSalePrice", "transPricePerSqm", 
                                            "scrapeDate", "latitude", "longitude", "id")
                    SELECT "immoweb_id", "customerName", "propType", "propStreet", "propHouseNo", 
                            "propLandSurface", "transSalePrice", "transPricePerSqm", 
                            "scrapeDate", "latitude", "longitude", "id"
                    FROM {cfg["table"]}
                    WHERE id = {id_}"""
        con.execute(text(sql))
        i += 1
    print(f"Inserted {i} record(s)")
    i = 0
    for immoweb_id, cm, surf, pps, pstr, phno, spr in zip(df_updated["immoweb_id"], df_updated["customerName_x"],
                                                          df_updated["propLandSurface_x"],
                                                          df_updated["transPricePerSqm_x"],
                                                          df_updated["propStreet_x"], df_updated["propHouseNo_x"],
                                                          df_updated["transSalePrice_x"]):
        sql = f"""  UPDATE {cfg["table"]}
                    SET "customerName" = '{cm}',
                        "propLandSurface" = {surf},
                        "transPricePerSqm" = {pps},
                        "propStreet" = '{pstr}',
                        "propHouseNo" = {phno},
                        "transSalePrice" = {spr},
                        "scrapeDate" = TO_TIMESTAMP({time.time()})
                    WHERE "immoweb_id" in ({immoweb_id}) """
        con.execute(text(sql))
        print(f"Updated id: {immoweb_id}")
        i += 1
    print(f"Updated {i} record(s)")
    con.commit()


def add_new_ids(con, scrape, server):
    scrape = scrape.rename(columns={'id': 'immoweb_id'})
    dfs_ = scrape[['immoweb_id', 'customerName', 'propType', 'propStreet', 'propHouseNo', 'propLandSurface',
                   'transSalePrice', 'transPricePerSqm', 'latitude', 'longitude']]
    dfs__ = dfs_[~dfs_['immoweb_id'].isin(server['immoweb_id'].tolist())]
    dfs__.to_sql(cfg["table"], con, if_exists='append', index=False)  # new immoweb_id's can be directly added
    print(f"{time.time()} -- Added {dfs__['immoweb_id'].count()} record(s)")
    con.commit()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('host', type=str, help="host address of Postgres database")
    args = parser.parse_args()
    df_scrap = automated_scraping([3270, 3271])
    # df_scrap = pd.read_csv("./3270_3271.csv")  # read in CSV for manual adjustments
    # df_scrap.to_csv("3270_3271.csv")

    conn, serv = create_con(args.host, cfg["db_pw"], cfg["db_user"])
    add_new_ids(con=conn, server=serv, scrape=df_scrap)
    update_existing_ids(con=conn, df_serv=serv, df_scrap=df_scrap)
    print(f"{time.time()} -- Done")
