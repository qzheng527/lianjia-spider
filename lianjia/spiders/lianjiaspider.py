# -*- coding: utf-8 -*-

import scrapy
import os
from lianjia.items import LianjiaItem

lianjia_url = "http://sh.lianjia.com/"
next_page_tag = 'results_next_page'


class LianjiaSpider(scrapy.Spider):
    name = "lianjiash"


    def start_requests(self):
        urls = [
            lianjia_url + 'ershoufang/shibobinjiang/',
            lianjia_url + 'ershoufang/longhua/',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        houses = response.css("ul.js_fang_list li")
        item = LianjiaItem()
        for house in houses:
            url = response.urljoin(house.css("div.prop-title a::attr(href)").extract_first())
            item['url'] = url
            item['id'] = os.path.basename(url).split('.')[0]
            item['desc'] = house.css("div.prop-title a::attr(title)").extract_first()
            item['price'] = house.css('span.total-price::text').extract_first()
            item['name'] = house.css("a.laisuzhou span::text").extract_first()
            yield item

        pages = response.css('div.c-pagination a')
        next_url =" "
        for page in pages:
            if page.css('::attr(gahref)').extract_first() == next_page_tag:
                next_url = response.urljoin(page.css('::attr(href)').extract_first())
                break
        print("========new page=========")
        if next_url != " ":
            yield scrapy.Request(next_url, callback=self.parse)
