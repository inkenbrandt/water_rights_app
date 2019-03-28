import requests
import pandas as pd

import importlib.util
spec = importlib.util.spec_from_file_location("dbconnect", "G:/My Drive/Python/dbconnect.py")
dbconnect = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dbconnect)
engine = dbconnect.postconn(port=5433, db = 'waterrights')


def get_use_data(location, location_type='CNT', **kwargs):
    # source_type = loc_type
    url = 'https://waterrights.utah.gov/asp_apps/waterUseData/postFilters.asp?'

    kwargs['format'] = 'json'
    kwargs['dataDisplay'] = 'allDataSelected'
    kwargs['measureUnits'] = 'acre feet'
    # kwargs['sourceTypeF'] = self.source_type
    kwargs['location'] = 'CNT'
    kwargs['countyName'] = location
    # kwargs['pwsUseOut'] = 'pwsUseOutSelected'
    # kwargs['indOutputSelect'] = 'indOutputSelected'
    # kwargs['mgtOutputSelect'] = 'mgtOutputSelected'
    utwr_dict = requests.get(url, params=kwargs)

    return utwr_dict


def pull_records(location):
    outdata = get_use_data(location)
    system = {}
    recrd = {}
    source = {}
    wtr_use = {}
    sys_colnames = ['SYSTEM_TYPE', 'SYSTEM_NAME', 'SYSTEM_ID', 'SYSTEM_STATUS', 'DEQ_ID', 'COUNTY']

    source_colnames = ['SYSTEM_ID', 'SOURCE_ID', 'SOURCE_STATUS', 'SOURCE_NAME', 'LAT', 'LON',
                       'SOURCE_TYPE', 'DIVERSION_TYPE', 'USE_TYPE']

    for i in outdata.json()['PWS_SYSTEMS']:
        system[int(i['SYSTEM_ID'])] = [i[col_name] for col_name in sys_colnames]
        for j in i['SOURCES']:
            source[int(j['SOURCE_ID'])] = [i['SYSTEM_ID']] + [j[key] for key in j.keys()][:-1]
            for k in j['RECORDS']:
                recrd["{:}-{:}".format(j['SOURCE_ID'], k['RECORD_YEAR'])] = [j['SOURCE_ID']] + [k[key] for key in
                                                                                                k.keys()]

        for m in i['WATER_USES']:
            wtr_use["{:}-{:}".format(i['SYSTEM_ID'], m['HISTORY_YEAR'])] = [i['SYSTEM_ID']] + [m[key] for key in
                                                                                               m.keys()]

    systems = pd.DataFrame.from_dict(system, orient='index', columns=sys_colnames).sort_index()
    systems = lowercols(systems)
    systems.index.name = 'systemid'
    systems = systems.drop(['system_id'], axis=1)
    systems['deq_id'] = systems['deq_id'].apply(
        lambda x: None if x.strip() == '' else pd.to_numeric(x, errors='coerce', downcast='integer'), 1)

    sources = pd.DataFrame.from_dict(source, orient='index', columns=['SYSTEM_ID'] + list(j.keys())[:-1])
    sources.index.name = 'sourceid'
    sources = lowercols(sources)
    sources['system_id'] = sources['system_id'].apply(lambda x: int(x), 1)
    sources['lat'] = sources['lat'].apply(lambda x: pd.to_numeric(x), 1)
    sources['lon'] = sources['lon'].apply(lambda x: pd.to_numeric(x), 1)
    sources = sources.drop(['source_id'], axis=1)

    recrds = pd.DataFrame.from_dict(recrd, orient='index', columns=['SOURCE_ID'] + list(k.keys())).sort_index()
    recrds = lowercols(recrds)
    recrds.index.name = 'recordid'
    records = recordunstack(recrds)

    wtr_uses = pd.DataFrame.from_dict(wtr_use, orient='index', columns=['SYSTEM_ID'] + list(m.keys())).sort_index()
    wtr_uses = lowercols(wtr_uses)
    wtr_uses.index.name = 'wateruseid'

    for col in wtr_uses.columns:
        if "use" in col:
            wtr_uses[col] = pd.to_numeric(wtr_uses[col])
        elif "connections" in col:
            wtr_uses[col] = wtr_uses[col].apply(lambda x: None if pd.isnull(x) or x.strip() == '' else int(x), 1)

    return systems, wtr_uses, sources, records


def recordunstack(recrds):
    unstack_records = recrds.drop(['total'], axis=1).reset_index().set_index(
        ['recordid', 'source_id', 'record_year']).stack().reset_index().rename(columns={'level_3': 'month', 0: 'wuse'})
    unstack_records['date'] = unstack_records[['record_year', 'month']].apply(
        lambda x: pd.to_datetime(str(x[1]).title() + " " + str(x[0]), format="%b %Y"), 1)
    unstack_records = unstack_records.drop(['recordid', 'month', 'record_year'], axis=1)
    unstack_records['sourcedateid'] = unstack_records[['source_id', 'date']].apply(
        lambda x: "{:}-{:%Y%m}".format(x[0], x[1]), 1)
    unstack_records.index.name = 'oldindex'
    unstack_records = unstack_records.reset_index().set_index('sourcedateid').drop(['oldindex'], axis=1)
    unstack_records['wuse'] = pd.to_numeric(unstack_records['wuse'])
    return unstack_records


def lowercols(df):
    df.columns = [str(i).lower() for i in df.columns]
    return df


tables = ['wtrsystem', 'wtruse', 'wtrsource', 'userecords']

systems, wtr_uses, sources, recrds = pull_records('Millard')
dfs = [systems, wtr_uses, sources, recrds]
for j in range(len(dfs)):
    dfs[j].to_sql(tables[j], engine, if_exists='append')
