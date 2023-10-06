import requests
import gzip
import csv
import os
import psycopg2
from dotenv import load_dotenv
from io import BytesIO, TextIOWrapper, StringIO
from loder_components.config import (job_types, workforce_types, od_table,
                                     od_temp_table, wac_table, wac_temp_table,
                                     rac_table, rac_temp_table)

load_dotenv()

HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
UN = os.getenv("UN")
PW = os.getenv("PW")

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
        self.__populate_tables('main_od')
        self.__populate_tables('aux_od')
        self.__populate_tables('wac')
        self.__populate_tables('rac')

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

        selected_job_types = picker(job_types)
        print(f"Selected Job Types: {selected_job_types}")

        selected_workforce_types = picker(workforce_types)
        print(f"Selected Workforce Segments: {selected_workforce_types}")

        return selected_job_types, selected_workforce_types

    def __create_tables(self):
        cursor, conn = self.__db_connect(self.lode_no)

        q1 = f"""
            create schema if not exists od;
            create table if not exists od.combined_od_table ({od_table});
        """

        q2 = f"""
            create schema if not exists wac;
            create table if not exists wac.combined_wac_table ({wac_table});
        """

        q3 = f"""
            create schema if not exists rac;
            create table if not exists rac.combined_rac_table ({rac_table});
        """

        cursor.execute(q1)
        cursor.execute(q2)
        cursor.execute(q3)
        cursor.close()
        conn.close()

    def __populate_tables(self, table: str):
        """Populates the created OD tables. Has to use temp table due to
           adding state info"""

        job_type = None
        segment = None

        if table == 'main_od' or table == 'aux_od':
            temp_table = od_temp_table
            sql_insert = f"""
                INSERT INTO od.combined_od_table 
                SELECT *, '{job_type}', '{self.state}', 'false', '{table}' FROM temp_table;
                """
        elif table == 'rac':
            temp_table = rac_temp_table
            sql_insert = f"""
                INSERT INTO rac.combined_rac_table
                SELECT *, '{self.state}', '{job_type}', '{segment}' FROM temp_table;
            """
        elif table == 'wac':
            temp_table = wac_temp_table
            sql_insert = f"""
                INSERT INTO wac.combined_wac_table
                SELECT *, '{self.state}', '{job_type}', '{segment}' FROM temp_table;
            """
        else:
            raise Exception("table must be main_od, aux_od, rac, or wac")

        urls = self.__create_urls(f'{table}')
        cursor, conn = self.__db_connect(self.lode_no)

        print(f'populating {table} table, please wait...')

        for key, value in urls.items():
            last_part = value.split("/")[-1].replace(".csv.gz", "")
            job_type, segment = self.__derive_type_and_seg(last_part)
            print(key, value)
            r = requests.get(value)
            if r.status_code == 200:
                compressed_file = BytesIO(r.content)
                decompressed_file = gzip.GzipFile(fileobj=compressed_file)
                reader = csv.reader(TextIOWrapper(decompressed_file, 'utf-8'))

                next(reader, None)  # Skip header

                cursor.execute(f"""
                    CREATE TEMP TABLE temp_table AS
                    {temp_table}
                """)

                buffer = StringIO()
                for row in reader:
                    buffer.write('\t'.join(row) + '\n')

                buffer.seek(0)

                sql_copy = """
                    COPY temp_table FROM stdin WITH DELIMITER '\t'
                """
                cursor.copy_expert(sql=sql_copy, file=buffer)

                cursor.execute(sql_insert)

                cursor.execute("DROP TABLE temp_table;")

            else:
                raise Exception(f'Could not download file at {value}')

        cursor.close()
        conn.commit()
        conn.close()

    def __derive_type_and_seg(self, key):
        try:
            url_parts = key.split('_')
            type = url_parts[3]
            seg = url_parts[2]
            return type, seg
        except IndexError:
            raise ValueError("Invalid URL format")

    def __create_urls(self, table: str):
        if table == 'main_od':
            table_base = self.base_url + 'od/'
            urls = {}
            for key in self.job_types:
                url = f'{self.state}_od_main_{key}_{self.year}.csv.gz'
                combined = table_base + url
                urls[key] = combined
        elif table == 'aux_od':
            table_base = self.base_url + 'od/'
            urls = {}
            for key in self.job_types:
                url = f'{self.state}_od_aux_{key}_{self.year}.csv.gz'
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
            raise Exception("table must be main_od, aux_od, rac, or wac")
        return urls


if __name__ == "__main__":
    a = PayLode(year, 'pa', 'lodes8')
    # a = PayLode(year, 'nj', 'lodes8')
