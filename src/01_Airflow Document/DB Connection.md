# DB Connection
## 1. MongoDB 
### 1) DB_CONN
- mongoAuth
- mongoHive

### 2) MONGO_DB
- aidery-auth
- aidery-health
- aidery-hive


```
##-- Code ---

SOURCES_MONGO_DB_CONN_ID = "mongoAuth"
SOURCES_MONGO_DB = "aidery-auth"
SOURCES_MONGO_COLLECTION = "domains"

mongoAuth = MongoHook(conn_id=SOURCES_MONGO_DB_CONN_ID) # connect db
domaColl = mongoAuth.get_collection(SOURCES_MONGO_COLLECTION,SOURCES_MONGO_DB) # get collection
domaColl.find({}).sort('_id', 1).sort('_id', 1).limit(100) # query

```

## 2. Postgres 

```

```



## 3. Redis

```
SOURCES_REDIS_CONN_ID = "redisCon"
redis_conn = RedisHook(redis_conn_id=SOURCES_REDIS_CONN_ID)
red_db = redis_conn.get_conn()

latestId = red_db.get('latestOfDomains')
# latestId = red_db.set('latestOfDomains', 'string_id')
print('latestId:', latestId)

```