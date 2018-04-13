import pymongo
import json
from bson import json_util, objectid
from cassandra.cluster import Cluster
from migration_utils import lineups_by_pool_ids, pool_rules, store_cassandra_entry, create_keyspace_and_table
import os
import pdb
import progressbar
import logging

mongo_url = os.environ['MONGO_URL']

query_chunk_size = int(os.getenv('QUERY_CHUNK_SIZE', '100'))
entry_idx_start = int(os.getenv('ENTRY_IDX_START', 0))

cassandra_ip = os.getenv('CASSANDRA_URL', 'localhost')
cassandra_keyspace = os.getenv('CASSANDRA_KEYSPACE', 'pool_history')

client = pymongo.MongoClient(mongo_url, ssl=True, username=os.environ['MONGO_USERNAME'], password=os.environ['MONGO_PASSWORD'])
cluster = Cluster([cassandra_ip])
session = cluster.connect()
create_keyspace_and_table(session, cassandra_keyspace)
session = cluster.connect(cassandra_keyspace)

pools_svc_db = client['pool-service']
pools_coll = pools_svc_db['pools']
entries_coll = pools_svc_db['entries']
lineups_coll = pools_svc_db['lineups']

event_svc_db = client['event-service']
events_coll = event_svc_db['events']

ingestion_gen_db = client['ingestion-gen']
ingestion_event_coll = ingestion_gen_db['event']

wallet_svc_db = client['wallet-service']
transactions_coll = wallet_svc_db['transactions']

hr_ingestion_gen_db = client['horseracing-generation']
hr_ingestion_event_coll = hr_ingestion_gen_db['events']

auth_svc_db = client['auth-service']
auth_role_coll = auth_svc_db['roles']
auth_principal_coll = auth_svc_db['principals']

bot_ids = []
bot_roles = list(auth_role_coll.find({"name": "bot"}))

if len(bot_roles) > 0:
    bot_role = bot_roles[0]
    bot_role_id = str(bot_role.get('_id'))
    bots = auth_principal_coll.find({"roles": bot_role_id})
    for bot in bots:
        if '_id' in bot:
            bot_ids.append(str(bot['_id']))

def filter_out_entries(entries):
    return_list = []
    for entry in entries:
        if 'userId' in entry and bot_ids.get(entry['userId']) is None:
            return_list.append(entry)

    return return_list

all_pool_ids = {}
all_event_ids = {}
all_remote_ids = {}
all_lineups_by_pool_id = {}
json_pool_id_mapping = {}


def print_json(obj):
    print(json_util.dumps(obj, sort_keys=True, indent=4, separators=(',', ': ')))


def memoized_pool(pool_id):
    json_pool = None
    if pool_id in json_pool_id_mapping:
        json_pool = json_pool_id_mapping[pool_id]
    else:
        if pool_id in all_lineups_by_pool_id:
            json_pool = json.dumps(all_lineups_by_pool_id[pool_id], sort_keys=True, indent=4, separators=(',', ': '))
            json_pool_id_mapping[pool_id] = json_pool
    return json_pool


def find_lineup_rank_by_entry_id(lineups, entry_id):
    for lineup in lineups:
        if lineup.get('entryId') == entry_id:
            return lineup.get("rank")
    return None

def split_into_hr_csgo_ids(ids):
    hr_ids = set()
    csgo_ids = set()
    for id in ids:
        if "sr:season" in id:
            csgo_ids.add(id)
        else:
            hr_ids.add(id)
    return (hr_ids, csgo_ids)


number_of_entries = entries_coll.count({"userId": {"$nin": bot_ids}})

bar = progressbar.ProgressBar(max_value=100)

print(f"Processing {number_of_entries} entries in chunks of {query_chunk_size} starting from {entry_idx_start}")
total_processed = 0
max_object = objectid.ObjectId("000000000000000000000000")

while True:
    entries = list(entries_coll.find({"_id": {"$gt": max_object}, "userId": {"$nin": bot_ids}}, limit=query_chunk_size).sort("_id", pymongo.ASCENDING))
    entries_count = len(entries)
    pool_ids = set()
    event_ids = set()
    entry_ids = set()
    remote_ids = set()

    max_object = entries[-1].get('_id')

    for entry in entries:
        pool_ids.add(str(entry.get('poolId')))
        event_ids.add(str(entry.get('eventId')))
        entry_ids.add(str(entry.get('_id')))

    missing_pool_ids = [objectid.ObjectId(p) for p in list(pool_ids.difference(all_pool_ids.keys()))]
    missing_event_ids = [objectid.ObjectId(e) for e in list(event_ids.difference(all_event_ids.keys()))]

    if len(missing_pool_ids) > 0:
        for pool in pools_coll.find({'_id': {"$in": missing_pool_ids}}):
            all_pool_ids[str(pool.get("_id"))] = pool
    if len(missing_event_ids) > 0:
        for event in events_coll.find({'_id': {"$in": missing_event_ids}}):
            all_event_ids[str(event.get("_id"))] = event
            remote_ids.add(event.get("remoteId"))

    missing_remote_ids = remote_ids.difference(set(all_remote_ids.keys()))

    if len(missing_remote_ids) > 0:
        (hr_remote_ids, csgo_remote_ids) = split_into_hr_csgo_ids(missing_remote_ids)
        if(len(csgo_remote_ids) > 0):
            for ingestion_event in ingestion_event_coll.find({"identifier": {"$in": list(csgo_remote_ids)}}):
                identifier = ingestion_event['identifier']
                all_remote_ids[identifier] = ingestion_event['metaData']
        if(len(hr_remote_ids) > 0):
            for hr_ingestion_event in hr_ingestion_event_coll.find({"remoteId": {"$in": list(hr_remote_ids)}}):
                identifier = hr_ingestion_event['remoteId']
                all_remote_ids[identifier] = hr_ingestion_event['metaData']

    smart_picks_for_entries = {}

    lineup_by_pool_id = lineups_by_pool_ids(lineups_coll, missing_pool_ids)
    all_lineups_by_pool_id.update(lineup_by_pool_id)

    transactions = list(transactions_coll.find({'txnType': "Credit", "metaData.entryId": {"$in": list(entry_ids)}}))

    for transaction in transactions:
        smart_picks_for_entries[transaction['metaData']['entryId']] = json.dumps(transaction['amount'])

    for entry in entries:
        default_pool = all_pool_ids.get(str(entry.get("poolId"))) or {}
        default_event = all_event_ids.get(str(entry.get("eventId"))) or {}
        meta_data = all_remote_ids.get(default_event.get("remoteId")) or all_remote_ids.get(str(default_event.get("_id")))
        meta_data_str = None
        if meta_data is not None:
            meta_data_str = json.dumps(meta_data)

        pool_id = str(entry.get("poolId"))
        entry_id = str(entry.get("_id"))
        rank = find_lineup_rank_by_entry_id(
            all_lineups_by_pool_id.get(pool_id, {}).get('_embedded', {}).get("lineups", []), entry_id)
        leaderboard_str = memoized_pool(pool_id)
        user_history_listing = {
                "userId": entry.get("userId"),
                "poolId": pool_id,
                "eventId": entry.get("eventId"),
                "entryId": entry_id,
                "challengeName": default_pool.get("name"),
                "challengeRules": set(pool_rules(default_pool)),
                "eventName": default_event.get("name"),
                "eventDate": default_event.get("startDate"),
                "poolSize": default_pool.get("poolSize"),
                "metaData": meta_data_str,
                "leaderboard": leaderboard_str,
                "rank": rank,
                "smartPicksWon": smart_picks_for_entries.get(entry_id),
                "won": smart_picks_for_entries.get(str(entry.get("_id"))) is not None,
                "discipline": default_event.get("discipline"),
                "status": None
        }
        if default_pool.get("status") == "Canceled":
            user_history_listing["status"] = "Canceled"
        elif default_pool.get("status") == "Past":
            user_history_listing["status"] = "Complete"
        else:
            user_history_listing["status"] = default_event.get("status")
        # print_json(user_history_listing)
        store_cassandra_entry(session, user_history_listing)

    total_processed += entries_count

    if entries_count < query_chunk_size:
        bar.update(100)
        break
    else:
        percent_complete = int((total_processed/number_of_entries) * 100)
        bar.update(percent_complete)
