#!/usr/bin/env python  
# -*- coding:utf-8 -*-  
""" 
@author: danerlt 
@file: get_value
@time: 2025-06-18
@contact: danerlt001@gmail.com
"""
import time

from nb_log import get_logger

from apollo_configs.client import ApolloClient

logger = get_logger("test_apollo_client")

# Apollo 配置中心测试服务器地址
APOLLO_TEST_SERVER = "http://81.68.181.139:8080/"
APOLLO_APP_ID = "my-test-config"
APOLLO_NAMESPACE = "dev.yml"
APOLLO_CLUSTER = "default"
APOLLO_SECRET = "ab0c0f0326d2413fafe41f87d61d028e"


def main():
    client = ApolloClient(
        meta_url=APOLLO_TEST_SERVER,
        app_id=APOLLO_APP_ID,
        app_secret=APOLLO_SECRET,
        cluster=APOLLO_CLUSTER,
        namespace=APOLLO_NAMESPACE,
    )
    index = 0
    while True:
        index += 1
        database = client.get_value("database")
        logger.info(f"index {index}, client._cache: {client._cache}")
        logger.info(f"index {index}, database: {database}")
        time.sleep(3)


if __name__ == '__main__':
    main()
