from google.cloud import pubsub_v1
import time
import json
import random

project_id = "ranjanrishi-project"
topic_id = "emp-trans-stream-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

names = ["Devranjan", "Rishidev", "Sarvesh"]
depts = ["IT", "HR", "Finance"]

while True:
    data = {
        "emp_id": random.randint(1, 1000),
        "name": random.choice(names),
        "dept": random.choice(depts),
        "sal": random.randint(30000, 80000)
    }

    publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
    print("Sent:", data)

    time.sleep(2)  # simulate streaming every 2 sec 