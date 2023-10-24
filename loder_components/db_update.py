import psycopg2
import os
from dotenv import load_dotenv
from .config import naics_cols

load_dotenv()

HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
UN = os.getenv("UN")
PW = os.getenv("PW")


def db_connect(db: str = "postgres"):
    """Boilerplate for connection params"""
    conn = psycopg2.connect(dbname=db, user=UN, password=PW, host=HOST, port=PORT)
    conn.autocommit = True
    cursor = conn.cursor()
    return cursor, conn


def build_index(db_name: str, counties: list, year: int):
    """Build an index to speed up later queries"""
    cursor, conn = db_connect(db_name)
    d = {"rac": "h_geocode", "wac": "w_geocode"}
    for key, value in d.items():
        print(f"building index for {key} table...")
        q = f"""create index if not exists {key}_index on {key}.combined_{key}_table({value});"""
        cursor.execute(q, {"counties": counties})
    print("building index for geography crosswalk...")
    cursor.execute(
        f"""create index if not exists xwalk_index on geo_xwalk.xwalk(tabblk{year})"""
    )
    print("building index for od table...")
    cursor.execute(
        "create index if not exists idx_home on od.combined_od_table(h_geocode);"
    )
    cursor.execute(
        "create index if not exists idx_work on od.combined_od_table(w_geocode);"
    )
    cursor.close()
    conn.close()


def local_flag(db_name: str, year: int, counties: list):
    """Sets the regional identifies column (dvrpc_reg) to true for the counties
    list passed into the class (default is dvrpc counties). For OD, the the flag
    is set for either/both home/work blocks where the block is in self.counties."""
    cursor, conn = db_connect(db_name)
    tables = ["rac", "wac", "od"]
    cols = ["w_geocode", "h_geocode"]
    for table in tables:
        init_q = f"update {table}.combined_{table}_table SET dvrpc_reg = false"
        cursor.execute(init_q)
        if table == "rac" or table == "wac":
            census_block_col = cols[1] if table == "rac" else cols[0]
            q = f"""update {table}.combined_{table}_table
                    set dvrpc_reg = case
                      when xwalk.ctyname = ANY(%(counties)s) then true
                      else false
                    end
                    from geo_xwalk.xwalk
                    where {table}.combined_{table}_table.{census_block_col} = geo_xwalk.xwalk.tabblk{year}
                    and xwalk.ctyname = ANY(%(counties)s)
                """
            print(f"updating dvrpc_reg column in {table}...")
            cursor.execute(q, {"counties": counties})

        elif table == "od":
            for col in cols:
                print(f"updating dvrpc_reg column in {table} for column {col}...")
                q = f"""update {table}.combined_{table}_table
                        set dvrpc_reg = case
                          when xwalk.ctyname = ANY(%(counties)s) then true
                          else false
                        end                
                        from geo_xwalk.xwalk
                        where {table}.combined_{table}_table.{col} = geo_xwalk.xwalk.tabblk{year}
                        and xwalk.ctyname = ANY(%(counties)s)
                    """
                cursor.execute(q, {"counties": counties})

    cursor.close()
    conn.close()


def build_regional_index(db_name: str):
    """Build an index to speed up later queries"""
    cursor, conn = db_connect(db_name)
    tables = ["rac", "wac", "od"]
    for table in tables:
        print(f"building regional index for {table} table...")
        q = f"""create index if not exists regional_{table}_index on {table}.combined_{table}_table(dvrpc_reg);"""
        cursor.execute(q)
    cursor.close()
    conn.close()


def add_dvrpc_cols(db_name: str, industry_threshold: float = 0.5):
    """Adds a few columns to the WAC and RAC tables for further DVRPC analysis.
    Populates the columns using other functions."""
    cursor, conn = db_connect(db_name)

    print(
        f"creating and populating dvrpc-created columns for {industry_threshold} industry threshold"
    )

    cols = {
        "dvrpc_block_significant_industry": "allnaics",
        "dvrpc_above_quartile_55": "ca03",
        "dvrpc_above_quartile_low_pay": "ce01",
        "dvrpc_above_quartile_no_hs": "cd01",
        "dvrpc_above_quartile_small_biz": "cfs01",
        "dvrpc_above_quartile_large_biz": "cfs05",
    }
    for key, value in cols.items():
        q = f"""alter table wac.combined_wac_table
                drop column if exists {key};
                alter table wac.combined_wac_table
                add column {key} bool default false;"""
        cursor.execute(q)
        if value == "allnaics":
            significant_industry(db_name, industry_threshold)
        else:
            dvrpc_quartiles(db_name, value, key)  # 55+ workers


def significant_industry(db_name: str, threshold: float = 0.75):
    """Tags true/false if an industry makes up more than threshold of the jobs in a block (default 75%)"""
    cursor, conn = db_connect(db_name)
    naics_str = " ".join([str(item + "+") for item in naics_cols])
    naics_str = naics_str.rstrip("+")
    query_blocks = []  # holds or clauses that are dynamically built below
    for index, value in enumerate(naics_cols):
        if index == 0:
            or_where = ""
        else:
            or_where = "or"

        or_clause = f"{or_where} {value} * 1.0 / ({naics_str}) > {threshold} \n\t"
        query_blocks.append(or_clause)

    q = f"""
        update wac.combined_wac_table
        set dvrpc_block_significant_industry = true
        where ({naics_str}) != 0 and 
        dvrpc_reg = true and (
        """
    for value in query_blocks:
        q = q + value
    q = q + ");"
    cursor.execute(q)
    cursor.close()
    conn.close()


def dvrpc_quartiles(db_name: str, col: str, boolcol: str, quartile: int = 4):
    """Returns the indicated quartile of the dvrpc region for a given column
    Top 75% = quartile 4. Note that 0 values are excluded.
    Example: top 75% of blocks with small biz versus top 75 of all blocks"""
    cursor, conn = db_connect(db_name)
    print(f"updating the {boolcol} column..")

    q = f"""
        with ranked_blocks as (
          select *, ntile(4) over (order by a.{col}) as quartile
          from wac.combined_wac_table a 
          where dvrpc_reg = true
          and {col} != 0
        ),
        minmax as (
            select min({col}) as min_value,
                max({col}) as max_value
                from ranked_blocks
                where quartile = 4
        )
        update wac.combined_wac_table
        set {boolcol} = true 
        from minmax
        where {col} >= minmax.min_value and {col} <= minmax.max_value
        """
    cursor.execute(q)
    cursor.close()
    conn.close()
