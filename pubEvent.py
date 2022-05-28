import time
from google.cloud import pubsub_v1

project_id = "norse-study-344420"
topic_id = ["search", "buy", "book"]
publisher = pubsub_v1.PublisherClient()


f = open("events.csv","r")
while True:
	line = f.readline()
	if not line:
	 	break

	
	sleepingTime,topic,message = line.split(",")

	sleepingTime = int(sleepingTime)
	message = message.replace("\n","")

	print("Publishing a topic: '%s' with message: %s"%(topic,message))

	# TODO PUB 
	if topic == "search":
		topic_path = publisher.topic_path(project_id, topic_id[0])
	elif topic == "buy":
		topic_path = publisher.topic_path(project_id, topic_id[1])
	else:
		topic_path = publisher.topic_path(project_id, topic_id[2])

	data_str = f"Topic: {topic}, Message: {message}"
	data= data_str.encode("utf-8")	
	future = publisher.publish(topic_path, data)
	print(future.result())	

	print("Waiting %i seconds"%sleepingTime)
	time.sleep(sleepingTime)

print("Done")


