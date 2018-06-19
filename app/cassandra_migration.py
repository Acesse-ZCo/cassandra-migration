import pymongo
import json
from bson import json_util, objectid
from cassandra.cluster import Cluster
from migration_utils import lineups_by_pool_ids, pool_rules, store_cassandra_entry, create_keyspace_and_table
import os
import pdb
import progressbar
import logging
from tqdm import tqdm
import psutil
import socket
from cachetools import LRUCache
from dateutil.parser import parse
import datetime
import time
import re
my_proc = psutil.Process(os.getpid())
mongo_url = os.environ['MONGO_URL']


query_chunk_size = int(os.getenv('QUERY_CHUNK_SIZE', '100'))
entry_idx_start = int(os.getenv('ENTRY_IDX_START', 0))

cassandra_url = os.getenv('CASSANDRA_URL', 'cassandra')

cassandra_urls = socket.gethostbyname_ex(cassandra_url)[2]
print(f"CASSANDRA_URL = {cassandra_urls}")
client = pymongo.MongoClient(mongo_url, ssl=True, username=os.environ['MONGO_USERNAME'], password=os.environ['MONGO_PASSWORD'])
cluster = Cluster(cassandra_urls)
cassandra_keyspace = os.getenv('CASSANDRA_KEYSPACE', 'pool_history')

query_obj = {}

start_date = os.getenv('START_DATE')
if start_date is not None:
    if len(start_date) < 13:
        start_date = start_date + "000"
    query_obj["zonedDateTime"] = {"$lt": int(start_date)}

pool_user_id = os.getenv('USER_ID')
if pool_user_id is not None:
    query_obj["userId"] = pool_user_id

write_async = os.getenv('WRITE_ASYNC')

if write_async == 'true':
    write_async = True
else:
    write_async = False

user_mapping=os.getenv('USER_MAPPING')

if user_mapping is not None and len(user_mapping.strip()) > 3:
    user_mapping_list = list(map(lambda x: x.strip().split(":"), user_mapping.split(',')))
    filtered_user_mapping_list = list(filter(lambda x: len(x) == 2, user_mapping_list))
    user_mapping = dict({(x[0].strip(), x[1].strip()) for x in filtered_user_mapping_list})


cassandra_session = cluster.connect()
create_keyspace_and_table(cassandra_session, cassandra_keyspace)
cassandra_session = cluster.connect(cassandra_keyspace)

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

bot_ids = {}
bot_roles = list(auth_role_coll.find({"name": "bot"}))

if len(bot_roles) > 0:
    bot_role = bot_roles[0]
    bot_role_id = str(bot_role.get('_id'))
    bots = auth_principal_coll.find({"roles": bot_role_id})
    for bot in bots:
        if '_id' in bot:
            bot_ids[str(bot['_id'])] = 1


def filter_out_entries(entries_list):
    return_list = []
    for list_entry in entries_list:
        if 'userId' in list_entry and bot_ids.get(list_entry['userId']) is None:
            return_list.append(list_entry)

    return return_list


max_cache_size = 50000

all_pool_ids = LRUCache(maxsize=max_cache_size)
all_event_ids = LRUCache(maxsize=max_cache_size)
all_remote_ids = LRUCache(maxsize=max_cache_size)
all_lineups_by_pool_id = LRUCache(maxsize=max_cache_size)
json_pool_id_mapping = LRUCache(maxsize=max_cache_size)


def print_json(obj):
    print(json_util.dumps(obj, sort_keys=True, indent=4, separators=(',', ': ')))


def memoized_pool(pool_id):
    json_pool = None
    if pool_id in json_pool_id_mapping:
        json_pool = json_pool_id_mapping[pool_id]
    else:
        if pool_id in all_lineups_by_pool_id:
            leaderboard = all_lineups_by_pool_id[pool_id]
            if '_embedded' in leaderboard and 'lineups' in leaderboard['_embedded']:
                lineups = leaderboard['_embedded']['lineups']
                lineups = sorted(lineups, key=lambda lineup: lineup.get('rank') or 99999)
                leaderboard['_embedded']['lineups'] = lineups
            json_pool = json.dumps(leaderboard, sort_keys=True, indent=4, separators=(',', ': '))
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

    return hr_ids, csgo_ids


number_of_entries = entries_coll.count(query_obj)

bar = progressbar.ProgressBar(max_value=100)

prize_regex = re.compile("^Prize\sAward")

print(f"Processing {number_of_entries} entries in chunks of {query_chunk_size} starting from {entry_idx_start}")

entries_processed = 0

with tqdm(range(0, number_of_entries, query_chunk_size), initial=entry_idx_start) as tq_bar:
    tq_bar.set_description('TOT MEM %dMB, MEM%%: %3.2f, Entries Processed: %d' % (my_proc.memory_info()[0]/(2**20), my_proc.memory_percent(), entries_processed))
    for entry_idx in tq_bar:
        entries = list(entries_coll.find(query_obj, skip=entry_idx, limit=query_chunk_size))
        entries_count = len(entries)
        pool_ids = set()
        event_ids = set()
        entry_ids = set()
        remote_ids = set()
        user_ids = set()

        for entry in entries:
            if 'userId' in entry and bot_ids.get(entry['userId']) is None:
                pool_ids.add(str(entry.get('poolId')))
                event_ids.add(str(entry.get('eventId')))
                entry_ids.add(str(entry.get('_id')))
                user_ids.add(entry.get('userId'))

        missing_pool_ids = [objectid.ObjectId(p) for p in list(pool_ids.difference(set(all_pool_ids.keys())))]
        missing_event_ids = [objectid.ObjectId(e) for e in list(event_ids.difference(set(all_event_ids.keys())))]

        if len(missing_pool_ids) > 0:
            for pool in pools_coll.find({'_id': {"$in": missing_pool_ids}}):
                all_pool_ids[str(pool.get("_id"))] = pool
        if len(missing_event_ids) > 0:
            for event in events_coll.find({'_id': {"$in": missing_event_ids}}):
                all_event_ids[str(event.get("_id"))] = event
                if event.get("discipline") == "HR" and "remoteId" in event:
                    all_remote_ids[event["remoteId"]] = event.get("metaData")
                else:
                    remote_ids.add(event.get("remoteId"))

        missing_remote_ids = remote_ids.difference(set(all_remote_ids.keys()))

        if len(missing_remote_ids) > 0:
            (hr_remote_ids, csgo_remote_ids) = split_into_hr_csgo_ids(missing_remote_ids)
            if len(csgo_remote_ids) > 0:
                for ingestion_event in ingestion_event_coll.find({"identifier": {"$in": list(csgo_remote_ids)}}):
                    identifier = ingestion_event['identifier']
                    all_remote_ids[identifier] = ingestion_event['metaData']

        smart_picks_for_entries = {}

        lineup_by_pool_id = lineups_by_pool_ids(lineups_coll, missing_pool_ids)
        all_lineups_by_pool_id.update(lineup_by_pool_id)

        #find_start_time = time.time()
        find_transactions = list(
            transactions_coll.find(
                {
                    "metaData.entryId": {"$in": list(entry_ids)},
                    "txnType": "Credit",
                    "$or": [{"metaData.code": "1100"}, {"desc": {"$regex": prize_regex}}]

                }
            ).hint([('metaData.entryId', 1)])
        )
        #find_end_time = time.time()
        #print("Time to process ", len(find_transactions), " transactions: ", find_end_time - find_start_time, " seconds")

        for transaction in find_transactions:
            amount = transaction.get('amount')

            if amount is not None:
                amount_value = amount.get('amount')
                if amount_value is not None:
                    amount_value = round(amount_value/100)
                amount['amount'] = amount_value

            smart_picks_for_entries[transaction['metaData']['entryId']] = json.dumps(amount)

        for entry in entries:
            if 'userId' in entry and bot_ids.get(entry['userId']) is None:
                default_pool = all_pool_ids.get(str(entry.get("poolId"))) or {}
                default_event = all_event_ids.get(str(entry.get("eventId"))) or {}
                meta_data = all_remote_ids.get(default_event.get("remoteId")) or all_remote_ids.get(str(default_event.get("_id")))
                meta_data_str = None
                if meta_data is not None:
                    if meta_data.get('roundType') is None:
                        meta_data['roundType'] = 'N/A'
                    if meta_data.get('round') is None and default_event.get("name") is not None:
                        event_name_round = default_event.get("name").split()[-1].strip()
                        if event_name_round.isdigit():
                            meta_data['round'] = event_name_round
                    meta_data_str = json.dumps(meta_data)

                pool_id = str(entry.get("poolId"))
                user_id = str(entry.get("_id"))
                rank = find_lineup_rank_by_entry_id(
                    all_lineups_by_pool_id.get(pool_id, {}).get('_embedded', {}).get("lineups", []), user_id)
                leaderboard_str = memoized_pool(pool_id)
                default_entry_id = entry.get("userId")
                if user_mapping is not None and default_entry_id in user_mapping:
                    default_entry_id = user_mapping[default_entry_id]

                user_history_listing = {
                        "userId": default_entry_id,
                        "poolId": pool_id,
                        "eventId": entry.get("eventId"),
                        "entryId": user_id,
                        "challengeName": default_pool.get("name"),
                        "challengeRules": set(pool_rules(default_pool)),
                        "eventName": default_event.get("name"),
                        "eventDate": default_event.get("startDate"),
                        "poolSize": default_pool.get("poolSize"),
                        "metaData": meta_data_str,
                        "leaderboard": leaderboard_str,
                        "rank": rank,
                        "smartPicksWon": smart_picks_for_entries.get(user_id),
                        "won": smart_picks_for_entries.get(str(entry.get("_id"))) is not None,
                        "discipline": default_event.get("discipline"),
                        "status": None
                }
                if default_event.get("status") == "Canceled":
                    user_history_listing["status"] = "Cancelled"
                elif default_event.get("status") == "Past":
                    user_history_listing["status"] = "Completed"
                else:
                    user_history_listing["status"] = default_event.get("status")
                store_cassandra_entry(cassandra_session, user_history_listing, async=write_async)
                entries_processed += 1

                if entries_processed % 10 == 0:
                    tq_bar.set_description('TOT MEM %dMB, MEM%%: %3.2f, Entries Processed: %d' % (my_proc.memory_info()[0]/(2**20), my_proc.memory_percent(), entries_processed))

print(f"Processed {entries_processed} entries")
