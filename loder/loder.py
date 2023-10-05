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
    def __init__(self, year: int, state: str, lode_no: str, part: str = 'main') -> None:
        self.state = state
        self.lode_no = lode_no
        self.base_url = f'https://lehd.ces.census.gov/data/lodes/{self.lode_no.upper()}/{self.state}/'
        self.year = year
        self.part = part
        self.job_types, self.workforce_types = self.__pick_tables()

        self.__create_db()
        self.__create_tables()
        self.__populate_od_tables()
        self.__populate_wac_tables()

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

    def __pick_tables(self):
        """Pick your tables"""

        def picker(table):
            for i, (key, value) in enumerate(table.items(), 1):
                print(f"{i}. {value} ({key})")
            choices = input(
                "Pick the tables you're interested in, separated by commas: ").split(',')
            selected = {list(table.keys())[int(choice.strip(
            )) - 1]: list(table.values())[int(choice.strip()) - 1] for choice in choices}
            return selected

        job_types = {
            'JT00': 'All Jobs',
            'JT01': 'Primary Jobs',
            'JT02': 'All Private Jobs',
            'JT03': 'Private Primary Jobs',
            'JT04': 'All Federal Jobs',
            'JT05': 'Federal Primary Jobs',
        }
        workforce_types = {
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

        selected_job_types = picker(job_types)
        print(f"Selected Job Types: {selected_job_types}")

        selected_workforce_types = picker(workforce_types)
        print(f"Selected Workforce Segments: {selected_workforce_types}")

        return selected_job_types, selected_workforce_types

    def __create_tables(self):
        for key in self.job_types:
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

                create schema if not exists wac;
                create table if not exists wac.combined_wac_table (
                    w_geocode char(15),
                    C000 numeric,
                    CA01 numeric,
                    CA02 numeric,
                    CA03 numeric,
                    CE01 numeric,
                    CE02 numeric,
                    CE03 numeric,
                    CNS01 numeric,
                    CNS02 numeric,
                    CNS03 numeric,
                    CNS04 numeric,
                    CNS05 numeric,
                    CNS06 numeric,
                    CNS07 numeric,
                    CNS08 numeric,
                    CNS09 numeric,
                    CNS10 numeric,
                    CNS11 numeric,
                    CNS12 numeric,
                    CNS13 numeric,
                    CNS14 numeric,
                    CNS15 numeric,
                    CNS16 numeric,
                    CNS17 numeric,
                    CNS18 numeric,
                    CNS19 numeric,
                    CNS20 numeric,
                    CR01 numeric,
                    CR02 numeric,
                    CR03 numeric,
                    CR04 numeric,
                    CR05 numeric,
                    CR07 numeric,
                    CT01 numeric,
                    CT02 numeric,
                    CD01 numeric,
                    CD02 numeric,
                    CD03 numeric,
                    CD04 numeric,
                    CS01 numeric,
                    CS02 numeric,
                    CFA01 numeric,
                    CFA02 numeric,
                    CFA03 numeric,
                    CFA04 numeric,
                    CFA05 numeric,
                    CFS01 numeric,
                    CFS02 numeric,
                    CFS03 numeric,
                    CFS04 numeric,
                    CFS05 numeric,
                    createdate char(8),
                    state char(2),
                    type char(4),
                    seg char(4)
                );

                """)
            cursor.close()
            conn.close()

    def __populate_od_tables(self):
        """Populates the created OD tables. Has to use temp table due to
           adding state info"""

        urls = self.__create_urls('od')
        cursor, conn = self.__db_connect(self.lode_no)

        print(
            f'populating od tables for {self.job_types} table(s)')

        for key, value in urls.items():
            print(key, value)
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

    def __populate_wac_tables(self):
        """Populates the created WAC tables. Has to use temp table due to
           adding state, type, and seg info"""

        urls = self.__create_urls('wac')
        cursor, conn = self.__db_connect(self.lode_no)

        print(f'populating wac tables for {self.job_types} table(s)')

        for key, value in urls.items():
            print(key, value)
            r = requests.get(value)
            last_part = value.split("/")[-1].replace(".csv.gz", "")
            wac_type, wac_seg = self.__derive_wac_type_and_seg(last_part)
            if r.status_code == 200:
                compressed_file = BytesIO(r.content)
                decompressed_file = gzip.GzipFile(fileobj=compressed_file)
                reader = csv.reader(TextIOWrapper(
                    decompressed_file, 'utf-8'))

                next(reader, None)  # Skip header

                cursor.execute("""
                    CREATE TEMP TABLE temp_table AS
                    select 
                        w_geocode,
                        C000,
                        CA01,
                        CA02,
                        CA03,
                        CE01,
                        CE02,
                        CE03,
                        CNS01,
                        CNS02,
                        CNS03,
                        CNS04,
                        CNS05,
                        CNS06,
                        CNS07,
                        CNS08,
                        CNS09,
                        CNS10,
                        CNS11,
                        CNS12,
                        CNS13,
                        CNS14,
                        CNS15,
                        CNS16,
                        CNS17,
                        CNS18,
                        CNS19,
                        CNS20,
                        CR01,
                        CR02,
                        CR03,
                        CR04,
                        CR05,
                        CR07,
                        CT01,
                        CT02,
                        CD01,
                        CD02,
                        CD03,
                        CD04,
                        CS01,
                        CS02,
                        CFA01,
                        CFA02,
                        CFA03,
                        CFA04,
                        CFA05,
                        CFS01,
                        CFS02,
                        CFS03,
                        CFS04,
                        CFS05,
                        createdate
                    FROM wac.combined_wac_table
                    WITH NO DATA;
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
                    INSERT INTO wac.combined_wac_table
                    SELECT *, '{self.state}', '{wac_type}', '{wac_seg}' FROM temp_table;
                """
                cursor.execute(sql_insert)

                cursor.execute("DROP TABLE temp_table;")

            else:
                raise Exception(f'Could not download file at {value}')

        cursor.close()
        conn.commit()
        conn.close()

    def __derive_wac_type_and_seg(self, key):
        try:
            url_parts = key.split('_')
            wac_seg = url_parts[2]
            wac_type = url_parts[3]
            return wac_type, wac_seg
        except IndexError:
            raise ValueError("Invalid URL format")

    def __create_urls(self, table: str):
        if table == 'od':
            table_base = self.base_url + 'od/'
            urls = {}
            for key in self.job_types:
                url = f'{self.state}_od_{self.part}_{key}_{self.year}.csv.gz'
                combined = table_base + url
                urls[key] = combined
        elif table == 'rac':
            table_base = self.base_url + 'rac/'
            urls = {}
            for key in self.job_types:
                for key2 in self.workforce_types:
                    f'{self.state}_rac_{self.workforce_types}_{self.job_types}_{self.year}.csv.gz'
                    url = f'{self.state}_rac_{key2}_{key}_{self.year}.csv.gz'
                    combined = table_base + url
                    urls[key] = combined
        elif table == 'wac':
            table_base = self.base_url + 'wac/'
            urls = {}
            for key in self.job_types:
                for key2 in self.workforce_types:
                    f'{self.state}_wac_{self.workforce_types}_{self.job_types}_{self.year}.csv.gz'
                    url = f'{self.state}_wac_{key2}_{key}_{self.year}.csv.gz'
                    combined = table_base + url
                    urls[key] = combined
        else:
            raise Exception("table must be od, rac, or wac")
        return urls


a = PayLode(year, 'pa', 'lodes8')
# a = PayLode(year, 'nj', 'lodes8')
