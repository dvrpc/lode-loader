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
LODES = os.getenv("LODES")
YEAR = os.getenv("YEAR")

counties = dvrpc_counties

for state in ["pa", "nj"]:
    PayLode(YEAR, state, LODES, DB, "all")

# build_index(LODES, counties, YEAR)
# local_flag(LODES, YEAR, counties)
# build_regional_index(LODES)
# add_dvrpc_cols(LODES, industry_threshold=0.75)
