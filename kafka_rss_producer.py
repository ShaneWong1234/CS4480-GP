from kafka import KafkaProducer
import time
import datetime
import pandas as pd
import feedparser
producer = KafkaProducer(bootstrap_servers='localhost:9092')
feed_url="https://feeds.bbci.co.uk/news/rss.xml"
last_updated_time = None

while True:
    newsfeed = feedparser.parse(feed_url)
    current_updated_time=datetime.datetime.fromtimestamp(time.mktime(newsfeed.feed.updated_parsed))
    if current_updated_time == last_updated_time:
        time.sleep(30)
        continue
    last_updated_time=current_updated_time

    for each in newsfeed.entries:    
        msg=f"{each.title}"
        print(msg)
        producer.send('news-raw',value=bytes(msg,encoding="UTF-8"))