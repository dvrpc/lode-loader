from loder_components.db_setup import PayLode
from loder_components.db_update import (
    build_index,
    local_flag,
    build_regional_index,
)
import os
import json
from dotenv import load_dotenv

load_dotenv()

DB = os.getenv("DB")
NEWDB = os.getenv("NEWDB")
SCHEMA = os.getenv("SCHEMA")
LODES = os.getenv("LODES")
YEAR = os.getenv("YEAR")
STATES = json.loads(os.getenv("STATES"))
COUNTIES = json.loads(os.getenv("COUNTIES"))


for state in STATES:
    PayLode(NEWDB, YEAR, state, LODES, DB, COUNTIES, "all", SCHEMA)

build_index(DB, COUNTIES, YEAR)
local_flag(DB, YEAR, COUNTIES)
build_regional_index(DB)
