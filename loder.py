from loder_components.db_setup import (
    PayLode
)
from loder_components.db_update import build_index, local_flag
from loder_components.config import dvrpc_counties

lode_no = 'lodes8'
year = 2020
counties = dvrpc_counties

# for state in ["pa", "nj"]:
# PayLode(year, state, lode_no, "all")

build_index(lode_no, counties, year)
local_flag(lode_no, year, counties)
