from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
from google.cloud import storage
import json

project_id = "norse-study-344420"
subscription_id = ["search-sub", "buy-sub", "book-sub"]
timeout = 1686.0 #segundos

subscriber = pubsub_v1.SubscriberClient()
storage_client = storage.Client()
bucket = storage_client.bucket("myunicorn")

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received {message}.")
    contents.append(f"{message})	
    message.ack()



contents = []
subscription_path = subscriber.subscription_path(project_id, subscription_id[0])
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")
f = open("search.csv", "w")
writer = csv.writer(f)
writer.writerows(contents)
blob = bucket.blob("search/search.csv") 
blob.upload_from_filename("search.csv")

subscription_path = subscriber.subscription_path(project_id, subscription_id[1])
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")
blob = bucket.blob("buy/buy.csv") 
blob.upload_from_string("contents")

subscription_path = subscriber.subscription_path(project_id, subscription_id[2])
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")
blob = bucket.blob("book/book.csv") 
blob.upload_from_string("contents")

with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()



