import jaydebeapi
import pandas as pd


class rmsone_odbc_client:
    JCLASSNAME = "com.cloudera.hive.jdbc41.HS2Driver"

    def __init__(self, url, username, password, jdbc_driver_path):
        self.url = url
        self.username = username
        self.password = password
        self.jdbc_driver_path = jdbc_driver_path

    def get_contract_tables(self):
        df = query_rms_one("SHOW TABLES")
        tables = df[df[0]].str.contains('contractprimaryrealpropertyview')
        return tables

    def get_contract_exposure(self, contract_table_name):
        query = """SELECT
            a.ADDRESSSCHEMEID1_ADDRESS_COUNTRYCODE as Country,
            a.ADDRESSSCHEMEID1_ADDRESS_admin1name as Admin1,
            a.ADDRESSSCHEMEID1_ADDRESS_admin2name as Admin2,
            a.ADDRESSSCHEMEID1_ADDRESS_POSTALCODE as ZIP,
            a.addressschemeid1_address_latitude as Latitude,
            a.addressschemeid1_address_longitude as Longitude,
            a.riskitem_numberofstories as NUMSTORIES,
            a.riskitem_yearbuilt as YEARBUILT,
            a.occupancy_occupancyschemename as OCCSCHEME,
            a.occupancy_occupancyname as OCCUPANCY,
            a.construction_constructionschemename as CONSTRSCHEME,
            a.construction_constructionname as CONSTRUCTION,
            a.risk_totaltiv as TIV

            FROM {tablename} a""".format(tablename=contract_table_name)
        df = query_rms_one(query)
        return df

    def query_rms_one(self, query):
        conn = jaydebeapi.connect(self.JCLASSNAME,
                                  self.url,
                                  [self.username, self.password],
                                  self.jdbc_driver_path)
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
        cursor.close()
        conn.close()
        return df
