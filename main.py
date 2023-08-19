import pymongo as pm
import redis
import json
import datetime
import pytz

uri = "mongodb+srv://cluster11.wcmxa8p.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority"
pem_file_path = "C:\\Users\\Jonathan YE\\Downloads\\X509-cert-5590899204222762842.pem"

# Get Mongo Client for Python Version 3.11.x
mongoClient = pm.MongoClient(uri, tls=True, tlsCertificateKeyFile=pem_file_path)

# To be used Planet Sample Data Set provided by Mongodb..
db = mongoClient["sample_guides"]

# Execute the query and retrieve the result
query_result = db.planets.find({"hasRings": True})

# password is fake. not actual password..
redisClient = redis.Redis(
    host="redis-13470.c302.asia-northeast1-1.gce.cloud.redislabs.com",
    port=13470,
    password="cwj36ddy3ddd38gfhjg",
)

# Create Pipeline Object here
pipeline = redisClient.pipeline()

# Process the query result
for index, document in enumerate(query_result, start=1):
    # Select indiviual fields to export
    jsonObject = {
        "name": document.get("name"),
        "orderFromSun": document.get("orderFromSun"),
        "mainAtmosphere": document.get("mainAtmosphere", []),
        "modifiedDate": datetime.datetime.now(pytz.timezone("Asia/Yangon")).isoformat(),
    }

    # Put json object through pipeline
    pipeline.set(index, json.dumps(jsonObject))

# Write to Redis Server
response = pipeline.execute()

# Reading data from redis server (Testing)
# Index of the document you want to read from Redis
keyToRead = 2

# Retrieve the JSON-encoded data from Redis
jsonData = redisClient.get(keyToRead)
print(json.loads(jsonData))

# Expected Json Output {'name': 'Saturn', 'orderFromSun': 6, 'mainAtmosphere': ['H2', 'He', 'CH4']}
