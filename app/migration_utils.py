import pymongo
from pymongo import MongoClient
from bson import json_util
import datetime
from cassandra import WriteTimeout, ConsistencyLevel
from cassandra.query import SimpleStatement
import logging

logging.basicConfig(filename='/tmp/cassandra-log.out', level=logging.DEBUG)

default_consistency = ConsistencyLevel.ANY

def create_keyspace_and_table(session, keyspace):
    session.execute(
        """
            CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}  AND durable_writes = true
        """.format(keyspace)
    )
    session.execute(
        """
            CREATE TABLE IF NOT EXISTS {}.usercontesthistoryrepository (
            userid text,
            poolid text,
            challengename text,
            challengerules set<text>,
            entryid text,
            eventdate bigint,
            eventid text,
            eventname text,
            leaderboard text,
            metadata text,
            poolsize int,
            rank int,
            smartpickswon text,
            status text,
            won boolean,
            discipline text,
            isconsistent boolean,
            PRIMARY KEY (userid, poolid)
        ) WITH CLUSTERING ORDER BY (poolid ASC)
            AND bloom_filter_fp_chance = 0.01
            AND caching = {{'keys': 'ALL', 'rows_per_partition': 'NONE'}}
            AND comment = ''
            AND compaction = {{'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}}
            AND compression = {{'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}}
            AND crc_check_chance = 1.0
            AND dclocal_read_repair_chance = 0.1
            AND default_time_to_live = 0
            AND gc_grace_seconds = 864000
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            AND read_repair_chance = 0.0
            AND speculative_retry = '99PERCENTILE';
            """.format(keyspace)
    )
    session.execute(
        """
        CREATE INDEX IF NOT EXISTS is_consistent_idx ON {}.usercontesthistoryrepository (isconsistent)
        """.format(keyspace)
    )


cassandra_insert_stmt = SimpleStatement(
    """
        INSERT INTO usercontesthistoryrepository (userid, poolid, challengename, challengerules, entryid, eventdate, eventid,
         eventname, leaderboard, metadata, poolsize, rank, smartpickswon, status, won, discipline, isconsistent)
            VALUES (%(userId)s, %(poolId)s, %(challengeName)s, %(challengeRules)s, %(entryId)s, %(eventDate)s, %(eventId)s, %(eventName)s,
             %(leaderboard)s, %(metaData)s, %(poolSize)s, %(rank)s, %(smartPicksWon)s, %(status)s, %(won)s, %(discipline)s, True)
    """, consistency_level=default_consistency
)

def store_cassandra_entry(session, user_contest_history):
    session.execute_async(cassandra_insert_stmt, user_contest_history)

def store_cassandra_entry_retry(session, user_contest_history, max_retries=5):
    future_session = session.execute_async(cassandra_insert_stmt, user_contest_history)
    try:
        future_session.result()
    except WriteTimeout:
        if max_retries > 0:
            print(f"Cassandra store failed. {max_retries} attempts left.")
            store_cassandra_entry(session, user_contest_history, consistency_level, max_retries - 1)
        else:
            print("Cassandra store failed. 0 retries left. Quitting")


def pool_rules(pool):
    pool_rules = []
    pool_size = pool['poolSize']
    if pool_size == 2:
        pool_rules.append("HEADTOHEAD")
    elif pool_size == 3:
        pool_rules.append("TRIO")
    elif pool_size == 30:
        pool_rules.append("CHALLENGE")
    elif pool_size > 2 and pool_size < 30:
        pool_rules.append("LEAGUE")
    elif pool_size > 30:
        pool_rules.append("TOURNAMENT")

    pool_cash = pool["prizes"]["cash"]
    if pool_cash['model'] == 'flat' and pool_size % 2 == 0 and pool_cash["ranks"] == pool_size / 2:
        pool_rules.append("5050CHALLENGE")

    if (pool.get("multiEntry") or False):
        pool_rules.append("MULTIENTRY")
    else:
        pool_rules.append("SINGLEENTRY")

    if (pool.get("guaranteed") or False):
        pool_rules.append("GUARANTEED")

    if pool["entryFee"]["amount"] == 0:
        pool_rules.append("FREE")

    return pool_rules


def convert_millis_to_iso(millis):
    return datetime.datetime.utcfromtimestamp(millis / 1000).isoformat() + "Z"


def transform_lineups(pool_id, lineups):
    returned_lineups = []
    for lineup in lineups:
        id_str = str(lineup["_id"])
        href = "/pools/" + pool_id + "/entries/" + lineup["userId"] + "/lineups/" + id_str
        del lineup["_id"]
        lineup["id"] = id_str
        lineup["entryId"] = str(lineup["entryId"])
        lineup["poolId"] = str(lineup["poolId"])
        if "players" in lineup:
            lineup['entries'] = lineup['players']
            del lineup["players"]
        lineup["_links"] = {"self": {"href": href}}
        if "created" in lineup:
            lineup["created"] = convert_millis_to_iso(lineup["created"])
        if "updated" in lineup:
            lineup["updated"] = convert_millis_to_iso(lineup["updated"])
        returned_lineups.append(lineup)
    return returned_lineups


def transform_link(elem):
    elem["total"] = elem["count"]
    pool_id = str(elem["_id"])
    href = "/pools/" + pool_id + "/leaderboard"
    elem["_links"] = {"self": {"href": href}}
    lineups = transform_lineups(pool_id, elem["lineups"])
    elem["_embedded"] = {"lineups": lineups}
    del elem["_id"]
    del elem["lineups"]
    return (pool_id, elem)


def lineups_by_pool_ids(lineup_coll, pool_oids):
    aggreg = lineup_coll.aggregate(
        [
            {"$match": {"poolId": {"$in": pool_oids}}},
            {"$group": {"_id": "$poolId", "count": {"$sum": 1}, "lineups": {"$push": "$$ROOT"}}}
        ],
        allowDiskUse=True
    )
    transf_aggreg_tuples = map(lambda elem: transform_link(elem), aggreg)
    transf_aggreg = dict((k, v) for k, v in transf_aggreg_tuples)
    return transf_aggreg


def test_aggreg_lineups():
    client = MongoClient()
    pools_svc_db = client['pool-service']
    entries_coll = pools_svc_db['entries']
    pool_oids = []

    for entry in entries_coll.find({}):
        if 'poolId' in entry:
            pool_oids.append(entry['poolId'])

    pool_oids = list(set(pool_oids))
    print(json_util.dumps(lineups_by_pool_ids(pools_svc_db['lineups'], pool_oids), sort_keys=True, indent=4,
                          separators=(',', ': ')))

# test_aggreg_lineups()
