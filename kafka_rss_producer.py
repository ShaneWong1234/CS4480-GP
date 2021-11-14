from kafka import KafkaProducer
import time
import pandas as pd
import feedparser
newsfeed = feedparser.parse("https://feeds.bbci.co.uk/news/rss.xml")
producer = KafkaProducer(bootstrap_servers='localhost:9092')

for each in newsfeed.entries:    
    msg=f"{each.title}"
    print(msg)
    producer.send('news-raw',value=bytes(msg,encoding="UTF-8"))