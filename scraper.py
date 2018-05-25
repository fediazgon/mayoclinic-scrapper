#!/usr/bin/env python3
import json
import logging
import re
from collections import defaultdict, OrderedDict

import scrapy
from scrapy.crawler import CrawlerProcess
from tqdm import tqdm

OUTPUT_FILE = 'mayo-diseases.json'
diseases = []


class MayoClinicSpider(scrapy.Spider):
    """
    This spider extracts the information of every disease published at

    https://www.mayoclinic.org/diseases-conditions/index

    Each disease, cause and risk factor is uniquely identified by an id the first time is encountered. For example,
    after crawling this page:

    https://www.mayoclinic.org/diseases-conditions/abdominal-aortic-aneurysm/symptoms-causes/syc-20350688

    The disease 'Abdominal aortic aneurysm' is given the id '1'. This diseases is then linked with the causes
    'Tobacco use' (id '1'), 'atherosclerosis' (id '2'), 'High blood pressure' (id '3'), etc. The same is done for the
    risk factors.

    In this way, if after crawling another disease, the cause 'Heredity' is encountered again, this new disease will be
    linked with the cause id '1'.

    In the end, a JSON Array is generated. Every element of this array have the following format::

        {
        "disease_id":1,
        "disease_name": "Abdominal aortic aneurysm",
        "causes": [ { "cause_id": 1, "cause_name": "Tobacco use" }, { "cause_id":2, "cause_name": "Atherosclerosis" } ],
        "risk_factors": [ { "risk_id":1, "risk_name":"Age"}, { "risk_id": 2, "risk_name": "Tobacco use" } ]
        }

    """
    name = 'mayo_clinic_spider'
    start_urls = ['https://www.mayoclinic.org/diseases-conditions/index']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        logging.getLogger('scrapy').setLevel(logging.WARNING)
        self.diseases = []
        self.diseases_id = defaultdict(lambda: len(self.diseases_id))
        self.causes_id = defaultdict(lambda: len(self.causes_id))
        self.risks_id = defaultdict(lambda: len(self.risks_id))

    def parse(self, response):
        """Find every link on the index page that links to a 'disease' page"""
        links = response.xpath('//li/a[re:test(@href, "/diseases-conditions/.*/symptoms-causes")]/@href').extract()
        with tqdm(total=len(links)) as pbar:
            for link in links:
                disease_name = re.match('/diseases-conditions/(.*)/symptoms-causes', link).group(1)
                pbar.set_description("Processing {}".format(disease_name))
                pbar.update(1)
                disease_page = response.urljoin(link)
                yield scrapy.Request(disease_page, callback=self.parse_disease)

    def parse_disease(self, response):
        """Extract the information from the 'disease' page"""
        disease_name = response.xpath('//h1/a/text()').extract()[0]
        causes_names = response.xpath('//h2[text()="Causes"]/following-sibling::ul[1]/li/strong/text()').extract()
        risks_names = response.xpath('//h2[text()="Risk factors"]/following-sibling::ul[1]/li/strong/text()').extract()
        diseases.append(
            OrderedDict([
                ('disease_id', self.diseases_id[disease_name]),
                ('disease_name', disease_name),
                ('causes', [
                    OrderedDict([
                        ('cause_id', self.causes_id[cause]),
                        ('cause_name', cause)
                    ])
                    for cause in map(lambda cause: cause[:-1], causes_names)
                ]),
                ('risk_factors', [
                    OrderedDict([
                        ('risk_id', self.risks_id[risk]),
                        ('risk_name', risk)
                    ])
                    for risk in map(lambda risk: risk[:-1], risks_names)
                ])
            ])
        )


process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(MayoClinicSpider)
process.start()

with open(OUTPUT_FILE, 'wt') as out:
    json.dump(diseases, out, indent=3)
