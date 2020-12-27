#!/usr/bin/env python
# coding: utf-8

import requests
from Logger import logger
from urllib.parse import quote
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Text, DateTime, DECIMAL, Float, Table, MetaData, UniqueConstraint, ForeignKey
from sqlalchemy.dialects.mysql import LONGTEXT
from datetime import datetime, date, timedelta
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.mysql import insert
import time
import random
import re
import configparser
from fake_useragent import UserAgent


class CConfg:
    def __init__(self, name):
        self._name = name
        self._db = ""
        self._start = ""
        self._end = ""

        # 读取配置文件
        cfg = configparser.ConfigParser()
        filename = cfg.read(filenames=self._name)
        if not filename:
            raise Exception('配置文件不存在，请检查后重启!')

        self._db = cfg.get('GLOBAL', 'db')

    @property
    def db(self):
        return self._db


cConfg = CConfg('config.ini')
engine = create_engine(cConfg.db, encoding='utf-8', echo=True)
conn = engine.connect()

metadata = MetaData()
t_bid = Table('higherschool', metadata,
              Column('id', Integer, primary_key=True, autoincrement=True),
              Column('feature_id', String(255)),
              Column('feature_name', Text()),
              Column('group_id', Text()),
              Column('group_name', Text()),
              Column('layer_id', Text()),
              Column('layer_name', Text()),
              Column('map_id', Text()),
              Column('longitude', Text()),
              Column('latitude', Text()),
              Column('tag_create_time', DateTime()),
              Column('tag_edit_time', DateTime()),
              Column('createtime', DateTime()),
              UniqueConstraint('feature_id', name='idx_feature_id')
              )


class Bid(object):
    def __init__(self, feature_id, feature_name, group_id, group_name, layer_id, layer_name, map_id, tag_create_time, tag_edit_time,
                 longitude, latitude):
        self.feature_id = feature_id
        self.feature_name = feature_name
        self.group_id = group_id
        self.group_name = group_name
        self.layer_id = layer_id
        self.layer_name = layer_name
        self.map_id = map_id
        self.longitude = longitude
        self.latitude = latitude
        self.tag_create_time = tag_create_time
        self.tag_edit_time = tag_edit_time
        self.createtime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def bid_upsert(bid):
    insert_stmt = insert(t_bid).values(
        feature_id=bid.feature_id,
        feature_name=bid.feature_name,
        group_id=bid.group_id,
        group_name=bid.group_name,
        layer_id=bid.layer_id,
        layer_name=bid.layer_name,
        map_id=bid.map_id,
        longitude=bid.longitude,
        latitude=bid.latitude,
        tag_create_time=bid.tag_create_time,
        tag_edit_time=bid.tag_edit_time,
        createtime=bid.createtime)
    # print(insert_stmt)

    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
        feature_id=insert_stmt.inserted.feature_id,
        feature_name=insert_stmt.inserted.feature_name,
        group_id=insert_stmt.inserted.group_id,
        group_name=insert_stmt.inserted.group_name,
        layer_id=insert_stmt.inserted.layer_id,
        layer_name=insert_stmt.inserted.layer_name,
        map_id=insert_stmt.inserted.map_id,
        longitude=insert_stmt.inserted.longitude,
        latitude=insert_stmt.inserted.latitude,
        tag_create_time=insert_stmt.inserted.tag_create_time,
        tag_edit_time=insert_stmt.inserted.tag_edit_time,
        createtime=insert_stmt.inserted.createtime,
        status='U')
    conn.execute(on_duplicate_key_stmt)


mapper(Bid, t_bid)
metadata.create_all(engine)


class LdmapSpider:
    def __init__(self):
        self.mapid = '44026eba-d624-437e-9185-1c6dfd0e1f70'

    def run_page(self, pagenumber, pagecount):
        # 构建请求头
        ua = UserAgent()
        headers = {
            'user-agent': ua.Chrome
        }

        url = 'http://www.ldmap.net/service/map/feature/list'
        # 请求url

        param = {
            'name': '',
            'layer': '',
            'state': -1,
            'pagenumber': pagenumber,
            'pagecount': pagecount,
            'mapid': self.mapid,
            '_': int(datetime.now().timestamp())
        }

        try:
            resp = requests.get(url, headers=headers, params=param)
        except requests.RequestException as e:
            logger.error(e)
        else:
            print(resp.text)
            if resp.status_code == 200:
                result = resp.json()
                for item in result['feature_list']:
                    tag_create_time = datetime.fromtimestamp(int(item['create_time'][6:19])/1000)
                    tag_edit_time = datetime.fromtimestamp(int(item['last_edit_time'][6:19])/1000)
                    feature_id = item['feature_id']
                    feature_name = item['feature_name']
                    group_id = item['group_id']
                    group_name = item['group_name']
                    layer_id = item['layer_id']
                    layer_name = item['layer_name']
                    map_id = item['map_id']
                    longitude, latitude = self.run_detail(feature_id)
                    print(longitude, latitude )
                    bid = Bid(tag_create_time=tag_create_time, tag_edit_time=tag_edit_time,
                              feature_id=feature_id, feature_name=feature_name, group_id=group_id, group_name=group_name,
                              layer_id=layer_id, layer_name=layer_name, map_id=map_id,
                              longitude=longitude, latitude=latitude)
                    bid_upsert(bid)

    def run_detail(self, feature_id):
        # 构建请求头
        ua = UserAgent()
        headers = {
            'user-agent': ua.Chrome
        }

        url = 'http://www.ldmap.net/service/map/feature/get'
        # 请求url

        param = {
            'feature_id': feature_id,
            'mapid': self.mapid,
            '_': int(datetime.now().timestamp())
        }

        try:
            resp = requests.get(url, headers=headers, params=param)
        except requests.RequestException as e:
            logger.error(e)
        else:
            print(resp.text)
            if resp.status_code == 200:
                result = resp.json()
                if 'point' in result:
                    latitude = result['point']['x']
                    longitude = result['point']['y']

        return longitude, latitude


if __name__ == '__main__':
    spider = LdmapSpider()
    for i in range(150):
        spider.run_page(i+20, 20)
