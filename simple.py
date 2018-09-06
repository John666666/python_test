
import redis

client = redis.Redis("120.55.184.130", password="Lu0b0tecDev")
print client.get("age")


