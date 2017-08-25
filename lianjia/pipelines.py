# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import shutil
import datetime
import csv
import codecs
import cStringIO
import pandas as pd

result_summary = 'result_summary.csv'
spider_result = "spider_result.csv"
temp_file = 'temp_file.csv'
header = [u"编号", u"链接", u"小区名", u"描述", u"价格"]

class UnicodeWriter:
    def __init__(self, f, dialect=csv.excel, encoding="utf-8-sig", **kwds):
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()
    def writerow(self, row):
        '''writerow(unicode) -> None
        This function takes a Unicode string and encodes it to the output.
        '''
        self.writer.writerow([s.encode("utf-8") for s in row])
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        data = self.encoder.encode(data)
        self.stream.write(data)
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

class LianjiaPipeline(object):
    def update_summary(self):
        print('Doing update ...')

        d1 = pd.read_csv(spider_result, index_col=False)
        d2 = pd.read_csv(result_summary, index_col=False)
        d3 = pd.merge(d1,d2,how='left')
        d3.to_csv(temp_file, index=False)

        shutil.copyfile(temp_file, result_summary)
        os.remove(temp_file)
        os.remove(spider_result)

    def process_item(self, item, spider):
        row =[]
        id = item['id']
        url = item['url']
        name = item['name']
        desc = item['desc']
        price = item['price']
        row.append(id)
        row.append(url)
        row.append(name)
        row.append(desc)
        row.append(price)

        self.writer.writerow(row)

    def open_spider(self, spider):
        if os.path.isfile(result_summary):
            self.update = 1
        else:
            self.update = 0
        self.csvFile = open(spider_result, "w")
        self.writer = UnicodeWriter(self.csvFile, delimiter=',')
        dt = datetime.datetime.now().strftime('%Y-%m-%d')
        header[4] = unicode(dt)
        self.writer.writerow(header)

    def close_spider(self, spider):
        self.csvFile.close()
        if self.update == 1:
            self.update_summary()
        else:
            shutil.copyfile(spider_result, result_summary)





