from collections.abc import Generator
from http.cookiejar import Cookie
import logging
from os.path import expanduser
from sys import exit
from time import sleep, strftime
from typing import Any
from urllib.parse import urlparse

import mariadb
import pandas as pd
import pywikibot as pwb
import requests


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s --- %(levelname)s --- %(message)s',
    datefmt='%Y-%m-%d, %H:%M:%S'
)

PREFIXES = {
    'SDCS' : 'https://commons.wikimedia.org/entity/statement/',
    'SDC' : 'https://commons.wikimedia.org/entity/',
    'SDCR' : 'https://commons.wikimedia.org/reference/',
    'WD' : 'http://www.wikidata.org/entity/',
    'WDT' : 'http://www.wikidata.org/prop/direct/',
    'PR' : 'http://www.wikidata.org/prop/reference/',
    'PQ' : 'http://www.wikidata.org/prop/qualifier/',
    'PS' : 'http://www.wikidata.org/prop/statement/',
}

# https://commons.wikimedia.org/wiki/Commons:Village_pump/Archive/2022/12#Depicts
WCQS_TOKEN_FILE = './token'
WCQS_ENDPOINT = 'https://commons-query.wikimedia.org/sparql'
WCQS_USER_AGENT = f'{requests.utils.default_user_agent()} (Wikidata bot' \
              ' by User:MisterSynergy; mailto:mister.synergy@yahoo.com)'
WCQS_CHUNK_SIZE = 10_000
WCQS_SLEEP = 2

REPLICA_PARAMS:dict[str, str] = {
    'host' : 'wikidatawiki.analytics.db.svc.wikimedia.cloud',
    'database' : 'wikidatawiki_p',
    'default_file' : f'{expanduser("~")}/replica.my.cnf'
}


class Replica:
    def __init__(self) -> None:
        self.replica = mariadb.connect(**REPLICA_PARAMS)
        self.cursor = self.replica.cursor(dictionary=True)

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.replica.close()


def query_mediawiki(query:str) -> list[dict[str, Any]]:
    with Replica() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

    return result


def query_mediawiki_to_dataframe(query:str, columns:list[str]) -> pd.DataFrame:
    df = pd.DataFrame(
        data=query_mediawiki(query),
        columns=columns,
    )

    return df


def query_deleted_items() -> pd.DataFrame:
    columns = [
        'qid',
        'admin',
        'ts',
    ]

    query = """WITH my_cte AS (
  SELECT
  	log_title,
  	MAX(log_timestamp) AS max_log_timestamp
  FROM
  	logging
  WHERE
    log_namespace=0
    AND log_type='delete'
    AND log_action='delete'
    AND log_title LIKE 'Q%'
    AND log_title NOT IN (
      SELECT
        page_title
      FROM
        page
      WHERE
        page_namespace=0
    )
  GROUP BY
    log_title
) SELECT
  CONVERT(l.log_title USING utf8) AS qid,
  CONVERT(a.actor_name USING utf8) AS admin,
  CONVERT(l.log_timestamp USING utf8) AS ts
FROM
  logging_userindex AS l
    JOIN actor_logging AS a ON l.log_actor=a.actor_id
    INNER JOIN my_cte AS g ON l.log_title=g.log_title AND l.log_timestamp=g.max_log_timestamp
ORDER BY
  CAST(SUBSTRING(l.log_title, 2) AS int) ASC"""

    df = query_mediawiki_to_dataframe(query, columns)

    return df.loc[df['qid'].str.startswith('Q')]


def init_wcqs_session(token:str) -> requests.Session:
    domain = urlparse(WCQS_ENDPOINT).netloc

    cookie = Cookie(
        0,
        'wcqsOauth',
        token,
        None,
        False,
        domain,
        False,
        False,
        '/',
        True,
        False,
        None,
        True,
        None,
        None,
        {}
    )

    session = requests.Session()
    session.headers.update({ 'User-Agent' : WCQS_USER_AGENT })
    session.cookies.set_cookie(cookie)

    return session


def query_wcqs(session:requests.Session, query:str) -> list[dict[str, Any]]:
    response = session.post(
        url=WCQS_ENDPOINT,
        data={
            'query': query
        },
        headers={
            'Accept': 'application/json'
        }
    )
    response.raise_for_status()

    payload = response.json()

    return payload.get('results', {}).get('bindings', [])


def chunk_list(lst:list, chunk_size:int=WCQS_CHUNK_SIZE) -> Generator[list, None, None]:
    for i in range(0, len(lst), chunk_size):
        logging.info(f'{strftime("%H:%M:%S")} --- {i}/{len(lst)} ({i/len(lst)*100:.2f}%)')
        yield lst[i:i+chunk_size]


def spot_invalid_references(session:requests.Session, s:pd.Series) -> list[str]:  # ref handles in WCQS that are no longer used
    query = f"""SELECT ?sdcref WHERE {{
  VALUES ?sdcref {{ sdcref:{' sdcref:'.join(s.tolist())} }}
  OPTIONAL {{ ?m ?p [ prov:wasDerivedFrom ?sdcref ] }}
  FILTER(!BOUND(?m)) .
}}"""

    payload = query_wcqs(session, query)
    logging.info(f'Found {len(payload)} results in reference node validation')

    invalid_references = []

    for row in payload:
        sdcref = row.get('sdcref', {}).get('value')
        sdcref = sdcref[len(PREFIXES.get('SDCR', '')):]
        invalid_references.append(sdcref)

    return invalid_references


def make_presentable_dataframe(df:pd.DataFrame, qids:pd.DataFrame) -> pd.DataFrame:
    df = df.merge(
        right=qids,
        how='left',
        left_on='item',
        right_on='qid',
    )

    df2 = df.loc[df['subject'].str.len()>12, ['item', 'admin', 'ts']].drop_duplicates()
    df2 = df2.merge(
        right=df.groupby(by='item').size().reset_index(),
        how='left',
        on='item'
    ).rename(columns={0:'cnt'})

    df2['timestamp'] = pd.to_datetime(df2['ts'], format='%Y%m%d%H%M%S')

    df2.to_feather('./results.feather')  # if something goes wrong, try to debug with this dump

    return df2


def make_table(df:pd.DataFrame) -> str:
    table = """{| class="wikitable sortable" style="margin:auto;"
|-
! item !! deleted by !! deletion time !! SDC uses
"""

    df = df.loc[df['item'].str.removeprefix('Q').astype(int).sort_values().index]  # sort by QID

    for tpl in df.itertuples():
        table += f"""|-
| [[{tpl.item}]] || [[User:{tpl.admin}|{tpl.admin}]] || {tpl.timestamp} || {tpl.cnt}
"""

    table += """|}"""

    return table


def make_report(table:str) -> str:
    report = f"""Update: <onlyinclude>{strftime("%Y-%m-%d, %H:%M")} (UTC)</onlyinclude>

In order to find usage, visit [https://commons-query.wikimedia.org/ WCQS], log in, and run a query such as: {{{{SPARQL|project=sdc|query=SELECT ?s ?p WHERE {{ ?s ?p wd:Q42 }} }}}}

The first column <code>?s</code> represents in most cases [[:mw:Extension:WikibaseMediaInfo#MediaInfo Entity|MediaInfo entities]] or SDC statement nodes. These links redirect to the file page that is using the queried Wikidata item via SDC. For (rare) usage in reference nodes, more sophisticated queries need to be run in order to find the page using the deleted entity.

{table}

[[Category:Database reports|Deleted Wikidata entities used in SDC]]"""

    return report


def write_to_wiki(report:str) -> None:
    site = pwb.Site('wikidata', 'wikidata')
    page = pwb.Page(site, 'Wikidata:Database reports/Deleted Wikidata entities used in SDC')
    page.text = report
    page.save(summary='upd', minor=False)
    #print(report)


def read_wcqs_token() -> str:
    with open(WCQS_TOKEN_FILE, mode='r', encoding='utf8') as file_handle:
        token = file_handle.read().strip()

    return token


def main() -> None:
    try:
        token = read_wcqs_token()
    except FileNotFoundError:
        logging.error('No token file found for WCQS credentials')
        exit('Token file with WCQS credentials not found')

    session = init_wcqs_session(token)

    query_template = """SELECT ?file ?predicate ?item WHERE {{
  VALUES ?item {{
    wd:{qids}
  }}
  ?file ?predicate ?item .
}}"""

    qids = query_deleted_items()
    qid_lst = qids['qid'].tolist()
    logging.info(f'Found {qids.shape[0]} deleted items')

    results:list[dict[str, str]] = []

    for sub_lst in chunk_list(qid_lst):
        query = query_template.format(qids=' wd:'.join(sub_lst))

        payload = query_wcqs(session, query)
        logging.info(f'Found {len(payload)} results')
        
        for row in payload:
            subject = row.get('file', {}).get('value')
            for prefix in PREFIXES.values():
                if not subject.startswith(prefix):
                    continue
                subject = subject[len(prefix):]
                subject_prefix = prefix

            predicate = row.get('predicate', {}).get('value')
            for prefix in PREFIXES.values():
                if not predicate.startswith(prefix):
                    continue
                predicate = predicate[len(prefix):]

            item = row.get('item', {}).get('value')[len(PREFIXES.get('WD', '')):]
            for prefix in PREFIXES.values():
                if not item.startswith(prefix):
                    continue
                item = item[len(prefix):]

            results.append(
                {
                    'subject' : subject,
                    'subject_prefix' : subject_prefix,
                    'predicate' : predicate,
                    'item' : item,
                }
            )

        sleep(WCQS_SLEEP)

    raw_df = pd.DataFrame.from_dict(data=results)

    invalid_references = spot_invalid_references(session, raw_df.loc[(raw_df['subject_prefix']==PREFIXES.get('SDCR', '')), 'subject'])
    raw_df = raw_df.loc[~raw_df['subject'].isin(invalid_references)]

    df = make_presentable_dataframe(raw_df, qids)
    logging.info(f'Found {df.shape[0]} cases to list on report page')

    table = make_table(df)

    report = make_report(table)

    write_to_wiki(report)
    logging.info(f'Report successfully written')


if __name__=='__main__':
    main()
