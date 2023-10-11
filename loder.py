from loder_components.db_setup import (
    PayLode
)

for state in ["pa", "nj"]:
    PayLode(2020, state, "lodes8", "all")


# def __build_index(self):
#     """Build an index to speed up later queries"""
#     if not PayLode.index_created:
#         cursor, conn = self.__db_connect(self.lode_no)
#         d = {'rac': 'h_geocode', 'wac': 'w_geocode'}
#         for key, value in d.items():
#             print(f'building index for {key} table...')
#             q = f"""create index if not exists {key}_index on {key}.combined_{key}_table({value});"""
#             cursor.execute(q, {'counties': self.counties})
#         print('building index for geography crosswalk...')
#         cursor.execute(
#             f"""create index if not exists xwalk_index on geo_xwalk.xwalk(tabblk{self.year})""")
#         print('building index for od table...')
#         cursor.execute(
#             'create index if not exists idx_home on od.combined_od_table(h_geocode);')
#         cursor.execute(
#             'create index if not exists idx_work on od.combined_od_table(w_geocode);')
#         cursor.close()
#         conn.close()
#         PayLode.index_created = True


# def __local_flag(self):
#     """Sets the regional identifies column (dvrpc_reg) to true for the counties
#        list passed into the class (default is dvrpc counties). For OD, the the flag
#        is set for either/both home/work blocks where the block is in self.counties."""

#     cursor, conn = self.__db_connect(self.lode_no)
#     tables = ['rac', 'wac', 'od']
#     cols = ['w_geocode', 'h_geocode']
#     for table in tables:
#         if table == 'rac':
#             census_block_col = cols[1]
#             where = f'where {table}.combined_{table}_table.{census_block_col} = geo_xwalk.xwalk.tabblk{self.year}'
#         elif table == 'wac':
#             census_block_col = cols[0]
#             where = f'where {table}.combined_{table}_table.{census_block_col} = geo_xwalk.xwalk.tabblk{self.year}'
#         elif table == 'od':
#             where = f"""where {table}.combined_{table}_table.{cols[0]} = geo_xwalk.xwalk.tabblk{self.year}
#                or {table}.combined_{table}_table.{cols[1]} = geo_xwalk.xwalk.tabblk{self.year} """

#         print(f'updating dvrpc_reg column in {table}...')

#         q = f"""update {table}.combined_{table}_table
#        set dvrpc_reg = true
#        from geo_xwalk.xwalk
#        {where}
#        and xwalk.ctyname = ANY(%(counties)s)
#     """

#         cursor.execute(q, {'counties': self.counties})

#     cursor.close()
#     conn.close()
