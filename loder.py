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
    def __init__(self, year: int, state: str, lode_no: str, pick_or_all: str = 'pick') -> None:
        self.state = state
        self.lode_no = lode_no
        self.base_url = f'https://lehd.ces.census.gov/data/lodes/{self.lode_no.upper()}/{self.state}/'
        self.pick_or_all = pick_or_all
        self.year = year
        self.job_types, self.workforce_types = self.__pick_tables()

        self.__drop_db()
        self.__create_db()
        self.__create_tables()
        self.__populate_tables('od_main')
        self.__populate_tables('od_aux')
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

    def __drop_db(self):
        cursor, conn = self.__db_connect()
        cursor.execute(f'drop database if exists {self.lode_no}')

    def __picker(self, table):
        all_choices = {}
        for i, (key, value) in enumerate(table.items(), 1):
            print(f"{i}. {value} ({key})")
            all_choices[key] = value
        choices = input(
            "Pick the tables you're interested in, separated by commas, or type 'a' for all: ").split(',')
        if choices[0] == 'a':
            selected = all_choices
            return selected
        else:
            selected = {list(table.keys())[int(choice.strip(
            )) - 1]: list(table.values())[int(choice.strip()) - 1] for choice in choices}
            return selected

    def __pick_tables(self):
        """Pick your tables"""
        if self.pick_or_all == 'all':
            selected_job_types = job_types
            selected_workforce_types = workforce_types
        else:
            selected_job_types = self.__picker(job_types)
            selected_workforce_types = self.__picker(workforce_types)

        print(f"Selected Job Types: {selected_job_types}")
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
        """Populates the created tables"""

        errors = []  # save any faulty urls here

        def handle_sql_insert(value):
            last_part = value.split("/")[-1].replace(".csv.gz", "")
            job_type, segment = self.__derive_type_and_seg(last_part)

            if table in ['rac', 'wac']:
                return f"""
                    INSERT INTO {table}.combined_{table}_table
                    SELECT *, '{self.state}', '{job_type}', '{segment}' FROM temp_table;
                """
            elif table in ['od_main', 'od_aux']:
                return f"""
                    INSERT INTO od.combined_od_table
                    SELECT *, '{job_type}', '{self.state}', 'false', '{table}' FROM temp_table;
                """
            else:
                return None

        if table == 'od_main' or table == 'od_aux':
            temp_table = od_temp_table
        elif table == 'rac':
            temp_table = rac_temp_table
        elif table == 'wac':
            temp_table = wac_temp_table
        else:
            raise Exception("table must be od_main, od_aux, rac, or wac")

        urls = self.__create_urls(f'{table}')
        cursor, conn = self.__db_connect(self.lode_no)

        # Check if the dictionary is nested
        if isinstance(urls, dict) and isinstance(next(iter(urls.values())), dict):
            iter_keys = urls.items()
        else:
            iter_keys = [(None, value)
                         for value in urls.values()]  # For flat dictionaries

        for outer_key, inner_dict in iter_keys:
            if outer_key is not None:  # For nested dictionaries
                inner_loop = inner_dict.items()
            else:  # For flat dictionaries
                inner_loop = [(None, inner_dict)]

            for inner_key, value in inner_loop:
                print(f'processing the {self.state} csv from {value}...')
                sql_insert = handle_sql_insert(value)
                if sql_insert:
                    r = requests.get(value)
                    if r.status_code == 200:
                        compressed_file = BytesIO(r.content)
                        decompressed_file = gzip.GzipFile(
                            fileobj=compressed_file)
                        reader = csv.reader(TextIOWrapper(
                            decompressed_file, 'utf-8'))

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
                        errors.append(value)

        cursor.close()
        conn.commit()
        conn.close()
        if len(errors) == 0:
            print(f'all {table} tables imported successfully!')
        elif len(errors) > 0:
            print(
                f'the following URLS might not exist: {errors}')
            for error in errors:
                error = error.split('_')
                job = error[3]
                seg = error[2]
                print(
                    f"there may be no '{job_types[job]}' in the '{workforce_types[seg]}' segment")
            print(
                'you can check to see if the tables actually exist at the endpoints below:')
            print(
                f'https://lehd.ces.census.gov/data/lodes/LODES8/{self.state}/od/')
            print(
                f'https://lehd.ces.census.gov/data/lodes/LODES8/{self.state}/rac/')
            print(
                f'https://lehd.ces.census.gov/data/lodes/LODES8/{self.state}/wac/')
            print(
                f'the rest of the {table} tables were imported successfully.')

    def handle_sql_insert(value, table, derive_type_and_seg_func, state):
        last_part = value.split("/")[-1].replace(".csv.gz", "")
        job_type, segment = derive_type_and_seg_func(last_part)

        if table in ['rac', 'wac']:
            return f"""
                INSERT INTO {table}.combined_{table}_table
                SELECT *, '{state}', '{job_type}', '{segment}' FROM temp_table;
            """
        elif table in ['od_main', 'od_aux']:
            return f"""
                INSERT INTO od.combined_od_table
                SELECT *, '{job_type}', '{state}', 'false', '{table}' FROM temp_table;
            """
        else:
            return None

    def __derive_type_and_seg(self, key):
        try:
            url_parts = key.split('_')
            type = url_parts[3]
            seg = url_parts[2]
            return type, seg
        except IndexError:
            raise ValueError("Invalid URL format")

    def __create_urls(self, table: str):
        if table == 'od_aux' or table == 'od_main':
            table_base = self.base_url + 'od/'
            urls = {}
            for key in self.job_types:
                url = f'{self.state}_{table}_{key}_{self.year}.csv.gz'
                combined = table_base + url
                urls[key] = combined
        elif table == 'rac' or table == 'wac':
            table_base = self.base_url + f'{table}/'
            urls = {}
            for key in self.job_types:
                urls[key] = {}
                for key2 in self.workforce_types:
                    url = f'{self.state}_{table}_{key2}_{key}_{self.year}.csv.gz'
                    combined = table_base + url
                    urls[key][key2] = combined
        else:
            raise Exception("table must be od_main, od_aux, rac, or wac")
        return urls


if __name__ == "__main__":
    # for state in ['pa', 'nj']:
    # PayLode(2020, state, 'lodes8', 'all')
    PayLode(2020, 'nj', 'lodes8')
