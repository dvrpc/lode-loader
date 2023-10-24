from loder_components.db_setup import PayLode
from loder_components.db_update import (
    build_index,
    local_flag,
    build_regional_index,
    add_dvrpc_cols,
)
from loder_components.config import dvrpc_counties
import os
from dotenv import load_dotenv

load_dotenv()

DB = os.getenv("DB")
YEAR = os.getenv("YEAR")

lode_no = DB
year = YEAR
counties = dvrpc_counties

for state in ["pa", "nj"]:
    PayLode(year, state, lode_no, "all")

build_index(lode_no, counties, year)
local_flag(lode_no, year, counties)
build_regional_index(lode_no)
add_dvrpc_cols(lode_no, industry_threshold=0.75)
