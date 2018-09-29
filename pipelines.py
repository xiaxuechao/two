# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html




from zhineng.settings import Mongo_Host,MONGO_PORT,MONGO_DB
from pymongo import MongoClient
import  datetime, re

class ZhinengPipeline(object):
    def __init__(self):
        # connection db
        self.conn = MongoClient(Mongo_Host, 27017)
        self.db = self.conn.get_database(MONGO_DB)

    def process_item(self, item, spider):

        #入库
        collection = self.db.get_collection(spider.MG_COLLECTION)
        for i in range(len(item[list(item.keys())[0]])):
            try:
                post_item = {}
                for field, val in item._values.items():
                    # if re.search(".*date", field):
                    #     post_item.update({field: '' if not val[0] else dateparser.parse(str(val[0]))})
                    # else:
                    post_item.update({field: val[i]})
                post_item.update({'create_date': datetime.datetime.now(), 'spider': spider.name})
                collection.save(post_item)

            except Exception as  e:
                print(e.args)
                spider.log('INSERT TO MGDB RRROR: %s' % e.args)

