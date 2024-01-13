import time
from immoweb_scraper import automated_scraping
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from config import config as cfg


def create_con(host, pw):
    url = URL.create(drivername="postgresql", username="automation", host=host, database="real_estate", password=pw)
    engine = create_engine(url)

    sql = f"""   SELECT * 
                FROM {cfg["table"]}"""
    df_serv = pd.read_sql(sql, con=engine.connect())
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
    print("Overview of ids:")
    print(df_updated['id'].to_list())
    con.commit()


def add_new_ids(con, scrape, server):
    scrape = scrape.rename(columns={'id': 'immoweb_id'})
    dfs_ = scrape[['immoweb_id', 'customerName', 'propType', 'propStreet', 'propHouseNo', 'propLandSurface',
                   'transSalePrice', 'transPricePerSqm', 'latitude', 'longitude']]
    dfs__ = dfs_[~dfs_['immoweb_id'].isin(server['immoweb_id'].tolist())]
    dfs__.to_sql(cfg["table"], con, if_exists='append', index=False)  # new immoweb_id's can be directly added
    print(f"Added {dfs__['immoweb_id'].count()} record(s)")
    con.commit()


if __name__ == "__main__":
    df_scrap = automated_scraping([3270, 3271])
    #df_scrap = pd.read_csv("./3270_3271.csv")  # read in CSV for manual adjustments
    #df_scrap.to_csv("3270_3271.csv")

    conn, serv = create_con("192.168.1.120", "G*2zHvP7yL8xQ")
    add_new_ids(con=conn, server=serv, scrape=df_scrap)
    update_existing_ids(con=conn, df_serv=serv, df_scrap=df_scrap)
    print("Done")
