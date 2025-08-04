# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from confluent_kafka import Producer
import json

class CrawlprojectPipeline:
    def __init__(self):
        self.producer = Producer({'bootstrap.servers': 'localhost:9092'})

    def process_item(self, item, spider):
        self.producer.produce('CrawlRes', json.dumps(item).encode('utf-8'))
        self.producer.flush()
        return item