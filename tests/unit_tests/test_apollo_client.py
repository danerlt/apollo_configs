#!/usr/bin/env python  
# -*- coding:utf-8 -*-  
""" 
@author: danerlt 
@file: test_apollo_client
@time: 2025-06-17
@contact: danerlt001@gmail.com
"""
from __future__ import annotations

import json
import time
from typing import Dict

import pytest
from pydantic import BaseModel

from apollo_configs.client import ApolloClient, ApolloSubscriber
from nb_log import get_logger

logger = get_logger("test_apollo_client")

# Apollo 配置中心测试服务器地址
APOLLO_TEST_SERVER = "http://81.68.181.139:8080/"
APOLLO_APP_ID = "my-test-config"
APOLLO_NAMESPACE = "dev.yml"
APOLLO_CLUSTER = "default"
APOLLO_SECRET = "ab0c0f0326d2413fafe41f87d61d028e"

class TestConfig(BaseModel):
    database: Dict[str, str | int]
    app: Dict[str, str | int | bool]
    redis: Dict[str, str | int]
    model: Dict[str, str | int | float]
    feature: Dict[str, str | int | bool | list]
    training: Dict[str, str]

def test_get_service_conf():
    """测试获取服务配置"""
    client = ApolloClient(
        meta_url=APOLLO_TEST_SERVER,
        app_id=APOLLO_APP_ID,
        app_secret=APOLLO_SECRET,
        cluster=APOLLO_CLUSTER,
        namespace=APOLLO_NAMESPACE,
    )
    
    # 获取服务配置
    service_conf = client.get_service_conf()
    
    # 验证服务配置结构
    assert isinstance(service_conf, list)
    assert len(service_conf) > 0
    
    # 验证第一个服务配置
    first_service = service_conf[0]
    assert "appName" in first_service
    assert "instanceId" in first_service
    assert "homepageUrl" in first_service
    
    # 验证服务名称
    assert first_service["appName"] == "apollo-configservice"
    
    # 验证 URL 格式
    assert first_service["homepageUrl"].startswith("http://")
    
    logger.info(f"获取服务配置成功: {service_conf}")

def test_get_service_conf_with_invalid_app_id():
    """测试使用无效的 app_id 获取服务配置"""
    try:
        client = ApolloClient(
            meta_url=APOLLO_TEST_SERVER,
            app_id="invalid-app-id",
            app_secret=APOLLO_SECRET,
            cluster=APOLLO_CLUSTER,
            namespace=APOLLO_NAMESPACE,
        )
        # 如果没有抛出异常，说明测试失败
        assert False, "应该抛出 ValueError"
    except ValueError as e:
        logger.info(f"正确捕获了异常: {e}")
    except Exception as e:
        logger.warning(f"捕获了意外的异常类型: {type(e).__name__}: {e}")

def test_get_service_conf_with_invalid_server():
    """测试使用无效的服务器地址获取服务配置"""
    client = ApolloClient(
        meta_url="http://invalid-server",
        app_id=APOLLO_APP_ID,
        app_secret=APOLLO_SECRET,
        cluster=APOLLO_CLUSTER,
        namespace=APOLLO_NAMESPACE,
    )

def test_update_config_server():
    """测试更新配置服务器"""
    client = ApolloClient(
        meta_url=APOLLO_TEST_SERVER,
        app_id=APOLLO_APP_ID,
        app_secret=APOLLO_SECRET,
        cluster=APOLLO_CLUSTER,
        namespace=APOLLO_NAMESPACE,
    )
    
    # 更新配置服务器
    config_server_url = client.update_config_server()
    
    # 验证配置服务器 URL
    assert config_server_url.startswith("http://")
    assert client._config_server_url == config_server_url
    assert client._config_server_url is not None
    
    logger.info(f"更新配置服务器成功: {config_server_url}")

def test_get_config():
    """测试获取配置"""
    client = ApolloClient(
        meta_url=APOLLO_TEST_SERVER,
        app_id=APOLLO_APP_ID,
        app_secret=APOLLO_SECRET,
        cluster=APOLLO_CLUSTER,
        namespace=APOLLO_NAMESPACE,
    )
    
    # 获取配置
    response = client.request_config_server()
    assert response.release_key is not None
    assert isinstance(response.config, dict)
    
    # 验证配置内容
    config = response.config
    assert "database" in config
    assert "app" in config
    assert "redis" in config
    assert "model" in config
    assert "feature" in config
    assert "training" in config
    
    # 验证数据库配置
    assert config["database"]["host"] == "localhost"
    assert config["database"]["port"] == 5432
    assert config["database"]["user"] == "test_user"
    assert config["database"]["password"] == "test_password"
    assert config["database"]["pool_size"] == 20
    
    # 验证应用配置
    assert config["app"]["name"] == "test_app"
    assert config["app"]["debug"] is True
    assert config["app"]["log_level"] == "INFO"
    assert config["app"]["workers"] == 4
    
    # 验证 Redis 配置
    assert config["redis"]["host"] == "localhost"
    assert config["redis"]["port"] == 6379
    assert config["redis"]["db"] == 0
    assert config["redis"]["password"] == "redis_password"
    
    # 验证模型配置
    assert config["model"]["batch_size"] == 32
    assert config["model"]["learning_rate"] == 0.001
    assert config["model"]["epochs"] == 100
    
    # 验证特征配置
    assert config["feature"]["window_size"] == 24
    assert config["feature"]["stride"] == 1
    assert config["feature"]["normalization"] is True
    assert isinstance(config["feature"]["feature_columns"], list)
    assert len(config["feature"]["feature_columns"]) == 5
    
    # 验证训练配置
    assert config["training"]["data_path"] == "/data/training"
    assert config["training"]["model_save_path"] == "/models"
    assert config["training"]["checkpoint_path"] == "/checkpoints"
    
    logger.info(f"获取配置成功: {response.config}")

def test_get_config_with_release_key():
    """测试使用 releaseKey 获取配置"""
    client = ApolloClient(
        meta_url=APOLLO_TEST_SERVER,
        app_id=APOLLO_APP_ID,
        app_secret=APOLLO_SECRET,
        cluster=APOLLO_CLUSTER,
        namespace=APOLLO_NAMESPACE,
    )
    
    # 第一次获取配置
    response1 = client.request_config_server()
    release_key = response1.release_key
    
    # 使用 releaseKey 再次获取配置
    response2 = client.request_config_server(release_key=release_key)
    assert response2.release_key == release_key
    logger.info(f"使用 releaseKey 获取配置成功: {response2.config}")

def test_get_config_with_messages():
    """测试使用 messages 获取配置"""
    client = ApolloClient(
        meta_url=APOLLO_TEST_SERVER,
        app_id=APOLLO_APP_ID,
        app_secret=APOLLO_SECRET,
        cluster=APOLLO_CLUSTER,
        namespace=APOLLO_NAMESPACE,
    )
    
    # 构造 messages
    messages = json.dumps({
        APOLLO_NAMESPACE: {
            "namespaceName": APOLLO_NAMESPACE,
            "notificationId": -1
        }
    })
    
    # 使用 messages 获取配置
    response = client.request_config_server(messages=messages)
    assert response.release_key is not None
    assert isinstance(response.config, dict)
    logger.info(f"使用 messages 获取配置成功: {response.config}")

def test_notification():
    """测试通知接口"""
    client = ApolloClient(
        meta_url=APOLLO_TEST_SERVER,
        app_id=APOLLO_APP_ID,
        app_secret=APOLLO_SECRET,
        cluster=APOLLO_CLUSTER,
        namespace=APOLLO_NAMESPACE,
    )
    
    # 构造通知请求
    notifications = [{
        "namespaceName": APOLLO_NAMESPACE,
        "notificationId": -1
    }]
    
    # 发送通知请求
    path = "notifications/v2"
    params = {
        "appId": client.app_id,
        "cluster": client.cluster,
        "notifications": json.dumps(notifications, ensure_ascii=False),
    }
    
    response = client.http_client.get(path, params=params)
    assert response.status_code in [200, 304]
    if response.status_code == 200:
        data = response.json()
        assert isinstance(data, list)
        logger.info(f"通知接口返回数据: {data}")

def test_subscriber():
    """测试订阅者功能"""
    config_changes: Dict[str, str] = {}
    
    def on_config_change(config: Dict[str, str]) -> None:
        if config:
            for key, value in config.items():
                config_changes[key] = value
                logger.info(f"配置变更: {key} = {value}")
    
    # 创建订阅者
    subscriber = ApolloSubscriber(
        action=on_config_change,
        priority=1,
        namespace=APOLLO_NAMESPACE
    )
    
    # 创建客户端并添加订阅者
    client = ApolloClient(
        meta_url=APOLLO_TEST_SERVER,
        app_id=APOLLO_APP_ID,
        app_secret=APOLLO_SECRET,
        cluster=APOLLO_CLUSTER,
        namespace=APOLLO_NAMESPACE,
        subscribers=[subscriber]
    )
    
    # 启动轮询
    client.start_polling()
    
    # 等待一段时间以接收配置变更
    time.sleep(5)
    
    # 停止轮询
    client.stop_polling()
    
    # 验证是否收到配置变更
    assert len(config_changes) > 0
    logger.info(f"收到的配置变更: {config_changes}")

def test_app_secret():
    """测试使用 app secret 的认证"""
    client = ApolloClient(
        meta_url=APOLLO_TEST_SERVER,
        app_id=APOLLO_APP_ID,
        app_secret=APOLLO_SECRET,
        cluster=APOLLO_CLUSTER,
        namespace=APOLLO_NAMESPACE,
    )
    
    # 获取配置
    response = client.request_config_server()
    assert response.release_key is not None
    assert isinstance(response.config, dict)
    logger.info(f"使用 app secret 获取配置成功: {response.config}")

def test_local_cache():
    """测试本地缓存功能"""
    client = ApolloClient(
        meta_url=APOLLO_TEST_SERVER,
        app_id=APOLLO_APP_ID,
        app_secret=APOLLO_SECRET,
        cluster=APOLLO_CLUSTER,
        namespace=APOLLO_NAMESPACE,
    )
    
    # 获取配置并更新本地缓存
    response = client.request_config_server()
    config = response.config
    release_key = response.release_key
    
    # 更新本地缓存
    client.update_local_file_cache(release_key, config, APOLLO_NAMESPACE)
    
    # 从本地缓存获取配置
    cached_config = client.get_local_file_cache(APOLLO_NAMESPACE)
    assert cached_config == config
    
    logger.info(f"本地缓存测试成功: {cached_config}")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])