#!/usr/bin/env python  
# -*- coding:utf-8 -*-  
""" 
@author: danerlt 
@file: client
@time: 2025-06-17
@contact: danerlt001@gmail.com
"""
from __future__ import annotations

import json
import os
import threading
import time
from typing import Callable, Dict, Sequence, Any

import yaml
from pydantic import BaseModel

from .http_client import HttpClient
from .logger import logger

ApolloConfig = Dict[str, "ApolloValue"]
DictConfig = Dict[str, Any]


class ApolloValue(BaseModel):
    value: Any
    update: bool


class ApolloServerResponse(BaseModel):
    release_key: str
    config: DictConfig


class ApolloSubscriber(BaseModel):
    action: Callable[[ApolloConfig], None]
    priority: int = 0
    namespace: str = "application"


class ApolloClient:
    _instances = {}
    _create_client_lock = threading.Lock()
    _update_cache_lock = threading.Lock()
    _cache_file_write_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        key = f"{args},{sorted(kwargs.items())}"
        with cls._create_client_lock:
            if key not in cls._instances:
                cls._instances[key] = super().__new__(cls)
        return cls._instances[key]

    def __init__(
            self,
            meta_url: str,
            app_id: str,
            cluster: str = "default",
            namespace: str = "application",
            polling_intervel: int = 2,
            polling_timeout: int = 90,
            subscribers: list[ApolloSubscriber] | None = None,
            app_secret: str | None = None,
            cache_file_dir_path: str | None = None,
    ):
        self.meta_url = meta_url
        self.app_id = app_id
        self.app_secret = app_secret
        self.cluster = cluster
        self.namespace = namespace
        self.polling_interval = polling_intervel
        self.polling_timeout = polling_timeout
        self._subscribers = subscribers or []
        self.check_subscribers()

        self.alive = True
        self.notification_id_map: dict[str, int] = {}
        self.configs: Dict[str, ApolloConfig] = {}

        # 初始化缓存相关属性
        self._cache: Dict = {}
        self._hash: Dict = {}
        self._config_server_url = None

        # 初始化缓存目录
        self._init_cache_file_dir_path(cache_file_dir_path)
        
        # 更新配置服务器
        self.update_config_server()
        
        # 初始化正式的 HttpClient
        self.http_client = HttpClient(self._config_server_url, app_id=app_id, app_secret=app_secret)
        
        # 获取配置
        self.fetch_configuration()

    def _init_cache_file_dir_path(self, cache_file_dir_path: str | None) -> None:
        """初始化缓存文件目录路径"""
        if cache_file_dir_path is None:
            self._cache_file_dir_path = os.path.join(
                os.path.abspath(os.path.dirname(__file__)), "config"
            )
        else:
            self._cache_file_dir_path = cache_file_dir_path

        # 确保缓存目录存在
        if not os.path.isdir(self._cache_file_dir_path):
            os.makedirs(self._cache_file_dir_path, exist_ok=True)

    def update_local_file_cache(self, release_key: str, data: str, namespace: str = "application") -> None:
        """更新本地缓存文件"""
        if self._hash.get(namespace) != release_key:
            with self._cache_file_write_lock:
                cache_file_path = os.path.join(
                    self._cache_file_dir_path,
                    f"{self.app_id}_configuration_{namespace}.txt",
                )
                with open(cache_file_path, "w", encoding="utf-8") as f:
                    new_string = json.dumps(data)
                    f.write(new_string)
                self._hash[namespace] = release_key

    def get_local_file_cache(self, namespace: str = "application") -> Dict:
        """从本地缓存文件获取配置"""
        cache_file_path = os.path.join(
            self._cache_file_dir_path, f"{self.app_id}_configuration_{namespace}.txt"
        )
        try:
            with open(cache_file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"读取缓存文件 {cache_file_path} 失败: {e}")
            return {}

    def update_cache(self, namespace: str, data: Dict) -> None:
        """更新缓存"""
        with self._update_cache_lock:
            self._cache[namespace] = data

    def get_service_conf(self) -> list:
        """获取配置服务器列表"""
        # 初始化临时 HttpClient 用于获取服务配置
        temp_http_client = HttpClient(self.meta_url, app_id=self.app_id, app_secret=self.app_secret)
        service_conf_path = f"services/config?appId={self.app_id}"
        logger.debug(f"service_conf_path: {service_conf_path}")
        response = temp_http_client.get(service_conf_path)
        if response.status_code != 200:
            raise ValueError(f"获取 Apollo 服务配置失败，状态码: {response.status_code}")
        
        try:
            service_conf = response.json()
            if not service_conf:
                raise ValueError("未找到 Apollo 服务")
            return service_conf
        except Exception as e:
            logger.error(f"解析 Apollo 服务配置失败: {e}")
            raise ValueError(f"解析 Apollo 服务配置失败: {e}")

    def update_config_server(self, exclude: str | None = None) -> str:
        """更新配置服务器信息"""
        service_conf = self.get_service_conf()
        logger.debug(f"Apollo 服务配置: {service_conf}")
        if exclude:
            service_conf = [
                service for service in service_conf if service["homepageUrl"] != exclude
            ]
        service = service_conf[0]
        self._config_server_url = service["homepageUrl"]

        logger.info(f"更新配置服务器 URL: {self._config_server_url}")

        return self._config_server_url

    def fetch_config_by_namespace(self, namespace: str = "application") -> None:
        """从 Apollo 服务器获取指定命名空间的配置"""
        url = f"configs/{self.app_id}/{self.cluster}/{namespace}"
        try:
            response = self.http_client.get(url)
            if response.status_code == 200:
                data = response.json()
                configurations = data.get("configurations", {})
                release_key = data.get("releaseKey", str(time.time()))
                self.update_cache(namespace, configurations)

                self.update_local_file_cache(
                    release_key=release_key,
                    data=configurations,
                    namespace=namespace,
                )
            else:
                logger.warning("从 Apollo 获取配置失败，从本地缓存文件加载")
                data = self.get_local_file_cache(namespace)
                self.update_cache(namespace, data)

        except Exception as e:
            data = self.get_local_file_cache(namespace)
            self.update_cache(namespace, data)

            logger.error(f"获取 Apollo 配置失败，错误: {e}, url: {url}, config server url: {self._config_server_url}")
            self.update_config_server(exclude=self._config_server_url)

    def fetch_configuration(self) -> None:
        """从 Apollo 服务器获取所有命名空间的配置"""
        try:
            self.fetch_config_by_namespace(self.namespace)
        except Exception as e:
            logger.warning(f"获取配置失败: {e}")
            self.load_local_cache_file()

    def load_local_cache_file(self) -> bool:
        """从本地缓存文件加载配置到内存"""
        try:
            for file_name in os.listdir(self._cache_file_dir_path):
                file_path = os.path.join(self._cache_file_dir_path, file_name)
                if os.path.isfile(file_path):
                    file_simple_name, file_ext_name = os.path.splitext(file_name)
                    if file_ext_name == ".swp":
                        continue
                    if not file_simple_name.startswith(f"{self.app_id}_configuration_"):
                        continue

                    namespace = file_simple_name.split("_")[-1]
                    with open(file_path) as f:
                        data = json.loads(f.read())
                        self.update_cache(namespace, data)
            return True
        except Exception as e:
            logger.error(f"加载本地缓存文件失败: {e}")
            return False

    def get_value(self, key: str, default_val: str | None = None, namespace: str = "application") -> str | None:
        """获取配置值"""
        try:
            if namespace in self._cache:
                return self._cache[namespace].get(key, default_val)
            return default_val
        except Exception as e:
            logger.error(f"获取键({key})值失败，错误: {e}")
            return default_val

    def get_json_value(self, key: str, default_val: dict | None = None, namespace: str = "application") -> dict:
        """获取配置值并转换为 JSON 格式"""
        val = self.get_value(key, namespace=namespace)
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            logger.error(f"键({key})的值不是 JSON 格式")

        return default_val or {}

    def request_config_server(self, release_key: str | None = None,
                              messages: str | None = None) -> ApolloServerResponse:
        path = f"configs/{self.app_id}/{self.cluster}/{self.namespace}"
        params = None
        if release_key and messages:
            params = {"releasekey": release_key, "messages": messages}

        response = self.http_client.get(path, params=params)
        response_data = response.json()
        configurations = response_data.get("configurations", {})
        namespace_type = self.namespace.split(".")[-1]
        if namespace_type == "yaml" or namespace_type == "yml":
            content = configurations.get("content")
            configs = yaml.safe_load(content)
        elif namespace_type == "json":
            content = configurations.get("content")
            configs = json.loads(content)
        # TODO add xml text 等类型
        else:
            configs = configurations
        return ApolloServerResponse(release_key=response_data["releaseKey"], config=configs)

    def update(self, server_response: ApolloServerResponse, namespace: str) -> None:
        if namespace not in self.configs:
            self.configs[namespace] = {}

        for key, value_in_server in server_response.config.items():
            if key in self.configs[namespace]:
                current_value = self.configs[namespace][key]
                if current_value.value != value_in_server:
                    logger.debug(f"更新配置 | {key}: {current_value.value} -> {value_in_server}")
                    self.configs[namespace][key] = ApolloValue(value=value_in_server, update=True)
                else:
                    self.configs[namespace][key] = ApolloValue(value=value_in_server, update=False)
            else:
                logger.debug(f"添加配置 | {key}: {value_in_server}")
                self.configs[namespace][key] = ApolloValue(value=value_in_server, update=True)

        self.notify()

    def check_subscribers(self) -> None:
        for subscriber in self._subscribers:
            if subscriber.namespace != self.namespace:
                raise ValueError(f"{subscriber.namespace} 与客户端命名空间 {self.namespace} 不匹配")

    def notify(self) -> None:
        self._subscribers = sorted(self._subscribers, key=lambda subscriber: subscriber.priority, reverse=True)
        for subscriber in self._subscribers:
            if subscriber.namespace is None:
                subscriber.action(None)  # type: ignore
            else:
                subscriber.action(self.configs[subscriber.namespace])

    def add_subscriber(self, subscriber: ApolloSubscriber) -> None:
        self._subscribers.append(subscriber)
        self.check_subscribers()

    def start_polling(self) -> None:
        self.alive = True
        long_polling_thread = threading.Thread(target=self._long_polling)
        long_polling_thread.daemon = True
        long_polling_thread.start()

    def stop_polling(self) -> None:
        self.alive = False

    def _long_polling(self) -> None:
        while self.alive:
            self._do_long_polling()
            time.sleep(self.polling_interval)

    def _do_long_polling(self) -> None:
        notifications = []
        notifications.append(
            {"namespaceName": self.namespace, "notificationId": self.notification_id_map.get(self.namespace, -1)})

        if not notifications:
            return

        try:
            path = "notifications/v2"
            params = {
                "appId": self.app_id,
                "cluster": self.cluster,
                "notifications": json.dumps(notifications, ensure_ascii=False),
            }

            response = self.http_client.get(path, params=params)
            if response.status_code == 304:
                logger.debug("配置未发生变化")
                return

            data = response.json()
            for entry in data:
                namespace = entry["namespaceName"]
                notification_id = entry["notificationId"]
                server_response = self.request_config_server()
                self.update(server_response, namespace=namespace)
                self.notification_id_map[namespace] = notification_id
                logger.debug(f"{namespace} 配置已更新，notification_id: {notification_id}")

        except Exception:
            logger.exception("长轮询失败")
