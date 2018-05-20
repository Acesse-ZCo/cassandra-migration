## Cassandra Migration Tool

#### Run as a docker container
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

### Options

#### Mongo Options
* MONGO_URL(required) - source for user contest data
* MONGO_USERNAME - required for authentication
* MONGO_PASSWORD - required for authentication
* CASSANDRA_URL(required) - cassandra hostname/ip address.
* CASSANDRA_KEYSPACE - defaults to 'pool_history'
* ENTRY_ID - optional comma separated list of entries to migrate
* WRITE_ASYNC - used to optionally write cassandra entries asychronously. Defaults to false
* QUERY_CHUNK_SIZE - number of entries processed per iteration. Defaults to 100
* USER_MAPPING - maps entry user ids to desired user ids
