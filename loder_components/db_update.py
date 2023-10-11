import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
UN = os.getenv("UN")
PW = os.getenv("PW")


def db_connect(self, db: str = "postgres"):
    """Boilerplate for connection params"""
    conn = psycopg2.connect(
        dbname=db, user=UN, password=PW, host=HOST, port=PORT)
    conn.autocommit = True
    cursor = conn.cursor()
    return cursor, conn


def build_index(lode_no: str, counties: list, year: int):
    """Build an index to speed up later queries"""
    cursor, conn = db_connect(lode_no)
    d = {'rac': 'h_geocode', 'wac': 'w_geocode'}
    for key, value in d.items():
        print(f'building index for {key} table...')
        q = f"""create index if not exists {key}_index on {key}.combined_{key}_table({value});"""
        cursor.execute(q, {'counties': counties})
    print('building index for geography crosswalk...')
    cursor.execute(
        f"""create index if not exists xwalk_index on geo_xwalk.xwalk(tabblk{year})""")
    print('building index for od table...')
    cursor.execute(
        'create index if not exists idx_home on od.combined_od_table(h_geocode);')
    cursor.execute(
        'create index if not exists idx_work on od.combined_od_table(w_geocode);')
    cursor.close()
    conn.close()


def __local_flag(self):
    """Sets the regional identifies column (dvrpc_reg) to true for the counties
       list passed into the class (default is dvrpc counties). For OD, the the flag
       is set for either/both home/work blocks where the block is in self.counties."""

    cursor, conn = self.__db_connect(self.lode_no)
    tables = ['rac', 'wac', 'od']
    cols = ['w_geocode', 'h_geocode']
    for table in tables:
        if table == 'rac':
            census_block_col = cols[1]
            where = f'where {table}.combined_{table}_table.{census_block_col} = geo_xwalk.xwalk.tabblk{self.year}'
        elif table == 'wac':
            census_block_col = cols[0]
            where = f'where {table}.combined_{table}_table.{census_block_col} = geo_xwalk.xwalk.tabblk{self.year}'
        elif table == 'od':
            where = f"""where {table}.combined_{table}_table.{cols[0]} = geo_xwalk.xwalk.tabblk{self.year}
               or {table}.combined_{table}_table.{cols[1]} = geo_xwalk.xwalk.tabblk{self.year} """

        print(f'updating dvrpc_reg column in {table}...')

        q = f"""update {table}.combined_{table}_table
       set dvrpc_reg = true
       from geo_xwalk.xwalk
       {where}
       and xwalk.ctyname = ANY(%(counties)s)
    """

        cursor.execute(q, {'counties': self.counties})

    cursor.close()
    conn.close()
