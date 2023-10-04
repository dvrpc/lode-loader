import requests
import gzip
import hashlib

base_url = 'https://lehd.ces.census.gov/data/lodes/LODES8/'
year = 2020
state = 'pa'


class PayLode:
    def __init__(self, base_url: str, year: int, state: str, part: str = 'main') -> None:
        self.base_url = base_url
        self.state = state
        self.year = year
        self.job_type = {
            'JT00': 'All Jobs',
            'JT01': 'Primary Jobs',
            'JT02': 'All Private Jobs', 'JT03': 'Private Primary Jobs',
            'JT04': 'All Federal Jobs',
            'JT05': 'Federal Primary Jobs',
        }
        self.workforce_segment = {
            'S000': 'Total number of jobs,',
            'SA01': 'Number of jobs of workers age 29 or younger',
            'SA02': 'Number of jobs for workers age 30 to 54',
            'SA03': 'Number of jobs for workers age 55 or older',
            'SE01': 'Number of jobs with earnings $1250/month or less',
            'SE02': 'Number of jobs with earnings $1251/month to $3333/month',
            'SE03': 'Number of jobs with earnings greater than $3333/month',
            'SI01': 'Number of jobs in Goods Producing industry sectors',
            'SI02': 'Number of jobs in Trade, Transportation, and Utilities industry sectors',
            'SI03': 'Number of jobs in All Other Services industry sectors',
        }

    def make_request(url: str):
        r = requests.get(url)
        return r.text

    def gzip_handler():
        pass

    def file_url(self):
        f'{self.state}_od_{self.part}_{self.job_type}_{self.year}.csv.gz'
        f'{self.state}_rac_{self.workforce_segment}_{self.job_type}_{self.year}.csv.gz'
        f'{self.state}_wac_{self.workforce_segment}_{self.job_type}_{self.year}.csv.gz'


a = PayLode(base_url, year, state)


# todo
# run psql to create db in script
# unzip a file and load into db
#
#
#
#
#
