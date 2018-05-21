## Cassandra Migration Tool

### Running the application

#### Running as a docker container
```
docker run \
  -e MONGO_URL=<mongo_url> \
  -e MONGO_USERNAME=<mongo_username> \
  -e MONGO_PASSWORD=<mongo_password> \
  -e QUERY_CHUNK_SIZE=17000 \
  -e CASSANDRA_URL=<cassandra_url> \
  -e CASSANDRA_KEYSPACE=<cassandra_keyspace> \
  -e ENTRY_ID=<entry_id> \
  -e WRITE_ASYNC=false \
  -e USER_MAPPING=user1:mapped_user1,user2:mapped_user2
  041479780340.dkr.ecr.us-east-1.amazonaws.com/zco/asports/cassandra-migration:0.0.1"
```

#### Running as part of a swarm
```
swarm service create  --restart-condition none \
    --with-registry-auth \
    --name cassandra_migration \
    --network data \
    --env MONGO_URL="mongodb://asports-stage-shard-00-00-gewvh.mongodb.net:27017,asports-stage-shard-00-01-gewvh.mongodb.net:27017,asports-stage-shard-00-02-gewvh.mongodb.net:27017/test?replicaSet=asports-stage-shard-0&authSource=admin" \
    --env MONGO_USERNAME=hmbadiwe \
    --env MONGO_PASSWORD=6tGvKyBPvstKGIYM \
    --env QUERY_CHUNK_SIZE=17000 \
    --env CASSANDRA_URL=tasks.cassandra \
    --env CASSANDRA_KEYSPACE='pool_history' \
    --env WRITE_ASYNC=false \
    --env ENTRY_ID=<entry_id> \
    --env WRITE_ASYNC=false \
    --env USER_MAPPING=user1:mapped_user1,user2:mapped_user2
    --replicas 1 \
        041479780340.dkr.ecr.us-east-1.amazonaws.com/zco/asports/cassandra-migration:0.0.1"

```


### Options

#### Mongo Options
* MONGO_URL(required) - source for user contest data
* MONGO_USERNAME - required for authentication
* MONGO_PASSWORD - required for authentication
* CASSANDRA_URL(required) - cassandra hostname/ip address.
* CASSANDRA_KEYSPACE - defaults to 'pool_history'
* ENTRY_ID - optional comma separated list of entries to migrate
* WRITE_ASYNC - used to optionally write cassandra entries asychronously to improve performance. Defaults to false
* QUERY_CHUNK_SIZE - number of entries processed per iteration. Defaults to 100
* USER_MAPPING - maps entry user ids to desired user ids
