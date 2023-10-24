import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
UN = os.getenv("UN")
PW = os.getenv("PW")


def db_connect(db: str = "postgres", schema: str = "public"):
    """Boilerplate for connection params"""
    conn = psycopg2.connect(
        dbname=db,
        user=UN,
        password=PW,
        host=HOST,
        port=PORT,
        options=f"-c search_path={schema}",
    )
    conn.autocommit = True
    cursor = conn.cursor()
    return cursor, conn


def build_index(db_name: str, counties: list, year: int, schema: str):
    """Build an index to speed up later queries"""
    cursor, conn = db_connect(db_name)
    d = {"rac": "h_geocode", "wac": "w_geocode"}
    for key, value in d.items():
        print(f"building index for {key} table...")
        q = f"""create index if not exists {key}_index on {schema}.combined_{key}({value});"""
        cursor.execute(q, {"counties": counties})
    print("building index for geography crosswalk...")
    cursor.execute(
        f"""create index if not exists xwalk_index on {schema}.xwalk(tabblk{year})"""
    )
    print("building index for od table...")
    cursor.execute(
        f"create index if not exists idx_home on {schema}.combined_od(h_geocode);"
    )
    cursor.execute(
        f"create index if not exists idx_work on {schema}.combined_od(w_geocode);"
    )
    cursor.close()
    conn.close()


def local_flag(db_name: str, year: int, counties: list, schema: str):
    """Sets the regional identifies column (dvrpc_reg) to true for the counties
    list passed into the class (default is dvrpc counties). For OD, the the flag
    is set for either/both home/work blocks where the block is in self.counties."""
    cursor, conn = db_connect(db_name)
    tables = ["rac", "wac", "od"]
    cols = ["w_geocode", "h_geocode"]
    for table in tables:
        init_q = f"update {schema}.combined_{table} SET dvrpc_reg = false"
        cursor.execute(init_q)
        if table == "rac" or table == "wac":
            census_block_col = cols[1] if table == "rac" else cols[0]
            q = f"""update {schema}.combined_{table}
                    set dvrpc_reg = case
                      when {schema}.xwalk.ctyname = ANY(%(counties)s) then true
                      else false
                    end
                    from {schema}.xwalk
                    where {schema}.combined_{table}.{census_block_col} = xwalk.tabblk{year}
                    and xwalk.ctyname = ANY(%(counties)s)
                """
            print(f"updating dvrpc_reg column in {schema}.{table}...")
            cursor.execute(q, {"counties": counties})

        elif table == "od":
            for col in cols:
                print(f"updating dvrpc_reg column in {table} for column {col}...")
                q = f"""update {schema}.combined_{table}
                        set dvrpc_reg = case
                          when xwalk.ctyname = ANY(%(counties)s) then true
                          else false
                        end                
                        from xwalk
                        where {schema}.combined_{table}.{col} = xwalk.tabblk{year}
                        and xwalk.ctyname = ANY(%(counties)s)
                    """
                cursor.execute(q, {"counties": counties})

    cursor.close()
    conn.close()


def build_regional_index(db_name: str, schema: str):
    """Build an index to speed up later queries"""
    cursor, conn = db_connect(db_name)
    tables = ["rac", "wac", "od"]
    for table in tables:
        print(f"building regional index for {table} table...")
        q = f"""create index if not exists regional_{table}_index on {schema}.combined_{table}(dvrpc_reg);"""
        cursor.execute(q)
    cursor.close()
    conn.close()
