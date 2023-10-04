import requests
import gzip
import csv
import os
import psycopg2
from dotenv import load_dotenv
from io import BytesIO, TextIOWrapper, StringIO

load_dotenv()

HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
UN = os.getenv("UN")
PW = os.getenv("PW")

base_url = 'https://lehd.ces.census.gov/data/lodes/LODES8/'
year = 2020


class PayLode:
    def __init__(self, base_url: str, year: int, state: str, lode_no: str, part: str = 'main') -> None:
        self.base_url = base_url
        self.state = state
        self.lode_no = lode_no
        self.year = year
        self.part = part
        self.job_type = {
            'JT00': 'All Jobs',
            'JT01': 'Primary Jobs',
            'JT02': 'All Private Jobs',
            'JT03': 'Private Primary Jobs',
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

        self.__create_db()
        self.__populate_od_tables()

    def __db_connect(self, db: str = 'postgres'):
        conn = psycopg2.connect(
            dbname=db, user=UN, password=PW, host=HOST, port=PORT)
        conn.autocommit = True
        cursor = conn.cursor()
        return cursor, conn

    def __create_db(self):
        cursor, conn = self.__db_connect()
        cursor.execute(
            f"select 1 from pg_database WHERE datname='{self.lode_no}'")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(f'create database {self.lode_no}')
        cursor.close()
        conn.close()

    # def __drop_tables(self):
    #     cursor, conn = self.__db_connect(self.lode_no)
    #     for key in self.job_type:
    #         cursor.execute(
    #             f"drop table if exists od.{self.part}_origin_destination_{key}")
    #     cursor.close()
    #     conn.commit()
    #     conn.close()

    def __create_od_tables(self):
        for key in self.job_type:
            cursor, conn = self.__db_connect(self.lode_no)
            cursor.execute(f"""
                create schema if not exists od;
                create table if not exists od.{self.part}_origin_destination_{key} (
                    w_geocode char(15) not null,
                    h_geocode char(15) not null,
                    s000 int,
                    sa01 int,
                    sa02 int,
                    sa03 int,
                    se01 int,
                    se02 int,
                    se03 int,
                    si01 int,
                    si02 int,
                    si03 int,
                    createdate char(8),
                    state char(2)
                    );
                """)
            cursor.close()
            conn.close()

    def __populate_od_tables(self):
        """Populates the created OD tables. Has to use temp table due to 
           adding state info"""

        self.__create_od_tables()
        urls = self.__od_urls()
        cursor, conn = self.__db_connect(self.lode_no)

        for key, value in urls.items():
            r = requests.get(value)
            if r.status_code == 200:
                compressed_file = BytesIO(r.content)
                decompressed_file = gzip.GzipFile(fileobj=compressed_file)
                reader = csv.reader(TextIOWrapper(decompressed_file, 'utf-8'))

                next(reader, None)  # Skip header

                cursor.execute(f"""
                    CREATE TEMP TABLE temp_table AS  
                    SELECT 
                        w_geocode, 
                        h_geocode, 
                        s000,     
                        sa01, 
                        sa02, 
                        sa03, 
                        se01,
                        se02,
                        se03,
                        si01,
                        si02,
                        si03,
                        createdate 
                    FROM od.{self.part}_origin_destination_{key} WITH NO DATA;
                """)

                buffer = StringIO()
                for row in reader:
                    buffer.write('\t'.join(row) + '\n')

                buffer.seek(0)

                sql_copy = """
                    COPY temp_table FROM stdin WITH DELIMITER '\t'
                """
                cursor.copy_expert(sql=sql_copy, file=buffer)

                sql_insert = f"""
                    INSERT INTO od.{self.part}_origin_destination_{key} SELECT *, '{self.state}' FROM temp_table;
                """
                cursor.execute(sql_insert)

                cursor.execute("DROP TABLE temp_table;")

            else:
                raise Exception(f'Could not download file at {value}')

        cursor.close()
        conn.commit()
        conn.close()

    def __od_urls(self):
        base_url = f'https://lehd.ces.census.gov/data/lodes/{self.lode_no.upper()}/{self.state}/od/'
        urls = {}
        for key in self.job_type:
            url = f'{self.state}_od_{self.part}_{key}_{self.year}.csv.gz'
            combined = base_url + url
            urls[key] = combined
        return urls


a = PayLode(base_url, year, 'pa', 'lodes8')
a = PayLode(base_url, year, 'nj', 'lodes8')

# url templates
# f'{self.state}_rac_{self.workforce_segment}_{self.job_type}_{self.year}.csv.gz'
# f'{self.state}_wac_{self.workforce_segment}_{self.job_type}_{self.year}.csv.gz'
