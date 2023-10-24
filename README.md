# loder
Pipeline to extract LEHD LODES tables into Postgres

## Setup/Installation

### Dependencies
requires python and postgresql. 

### Installation
#### 1. First, create a virtual environment at the project root.
```shell
python -m venv ve
```
#### 2. Activate it with:
```shell
. ve/bin/activate
```
(or `ve\Scripts\Activate.bat` for windows command prompt)

#### 3. Install python dependencies with:
```shell
pip install -r requirements.txt
```

#### 4. Create a .env file, with these variables. 
The lodes variable should be "lodes8" or another valid lode set, as this name is used in building URLS to the lodes endpoints.

```
HOST = "localhost"
UN = "your_postgres_un"
PW = "fake_pw"
PORT = "your_db_port"
DB = "db_name"
LODES = "lodes8"
YEAR = 2020
```

## Usage
Run the loder.py file. By default, it imports all tables then calculates some new values.
This takes a while, probably 1-3 hours depending on your internet and RAM.

### Data
The columns from the raw data are viewable [here.](https://lehd.ces.census.gov/data/lodes/LODES8/LODESTechDoc8.0.pdf)
This includes some good general info.

At the high level:
* The OD table captures origin and destination zones for workers (home zones and work zones). There is a scope column (values = od_main or od_aux). od_main only includes the states used in loder.py.
od_aux includes blocks from other states, so you can see flows from areas outside of the state(s) you're analyzing. Be sure to filter to one or the other.
* The RAC table includes jobs totaled by home census blocks.
* The WAC table includes jobs totaled by work census blocks. 

Here's an example sql query, which aggregates total jobs from the WAC table by tract.

```
select b.trct, sum(a.c000) as total_jobs from wac.combined_wac_table a
inner join geo_xwalk.xwalk b
on a.w_geocode = b.tabblk2020 
where job_type = 'JT00' -- all job types. other types include private, public, etc, see link
and segment = 'S000' -- total number of jobs, no segmentation. other segments use naics codes, age, etc..., see link 
and state = 'pa'
and dvrpc_reg = true -- filter to just DVRPC region
group by b.trct

```
It's important to add the job_type and segment to the query, otherwise you'll have duplicate blocks. It's helpful to add a state. 

Notice that the joined table is the geography crosswalk. The columns in that table are viewable in the link at the top of this section.
You can use that table to group by tract, ZCTA, county, or a number of other geographies. 

It's a good idea to group by census tract or block group; this data is at the block level which has a higher MOE.


## License
This project uses the GNU(v3) public license.