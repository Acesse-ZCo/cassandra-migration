import pymongo, os
import json
from bson import json_util, objectid

mongo_url = os.environ['MONGO_URL']


client = pymongo.MongoClient(mongo_url, ssl=True, username=os.environ['MONGO_USERNAME'], password=os.environ['MONGO_PASSWORD'])

auth_svc_db = client['auth-service']
auth_role_coll = auth_svc_db['roles']
auth_principal_coll = auth_svc_db['principals']

pools_svc_db = client['pool-service']
entries_coll = pools_svc_db['entries']

bot_ids = set()
bot_roles = list(auth_role_coll.find({"name": "bot"}))

if len(bot_roles) > 0:
    bot_role = bot_roles[0]
    bot_role_id = str(bot_role.get('_id'))
    bots = auth_principal_coll.find({"roles": bot_role_id})
    bot_ids = list(set(map(lambda x: str(x.get("_id")), bots)))


entries =  entries_coll.find({"userId": {"$nin": bot_ids}}).count()

print(f"There are a total number of {entries} entries")
