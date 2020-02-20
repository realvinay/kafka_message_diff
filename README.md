# Kafka message diff

Kafka message diff is a service that can listen to a topic with json messages and generate
a diff of messages based on a key. The json diff - creation, updations, deletions can be transmitted
on another kafka topic. 