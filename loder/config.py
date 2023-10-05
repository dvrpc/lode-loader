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

od_table = {
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
}

rac = {

}
wac = {

}

table_config = [job_types, workforce_types, od, rac, wac]
