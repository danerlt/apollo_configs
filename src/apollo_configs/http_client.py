#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: danerlt
@file: http_client
@time: 2025-06-17
@contact: danerlt001@gmail.com
"""

from __future__ import annotations

import base64
import functools
import hashlib
import hmac
import json
import time
from typing import Any, Callable, Dict, Optional, TypeVar, cast
from urllib.parse import urlparse

import requests
from requests import Response

from .logger import logger

T = TypeVar("T", bound=Callable[..., Response])


def log_http_request(func: T) -> T:
    @functools.wraps(func)
    def wrapper(self: "HttpClient", *args: Any, **kwargs: Any) -> Response:
        method = kwargs.get("method") or func.__name__.upper()
        path = args[0] if args else kwargs.get("path", "")
        url = f"{self.base_url}/{path.lstrip('/')}"

        # 获取请求参数
        params = kwargs.get("params")
        data = kwargs.get("data")
        json_data = kwargs.get("json_data")
        headers = kwargs.get("headers")

        # 记录请求日志
        log_parts = [
            f"请求方法: {method}",
            f"请求URL: {url}",
        ]

        if params:
            log_parts.append(
                f"请求参数: {json.dumps(params, ensure_ascii=False, indent=2)}"
            )
        if headers:
            log_parts.append(
                f"请求头: {json.dumps(headers, ensure_ascii=False, indent=2)}"
            )
        if data:
            log_parts.append(f"请求数据: {data}")
        if json_data:
            log_parts.append(
                f"请求JSON: {json.dumps(json_data, ensure_ascii=False, indent=2)}"
            )

        logger.debug(f"发送HTTP请求:\n{chr(10).join(log_parts)}")

        # 执行请求
        response = func(self, *args, **kwargs)

        # 记录响应日志
        try:
            response_data = response.json()
            response_content = json.dumps(response_data, ensure_ascii=False, indent=2)
        except:
            response_content = response.text

        response_log_parts = [
            f"响应状态码: {response.status_code}",
            f"响应头: {json.dumps(dict(response.headers), ensure_ascii=False, indent=2)}",
            f"响应内容: {response_content}",
        ]
        logger.debug(f"收到HTTP响应:\n{chr(10).join(response_log_parts)}")

        return response

    return cast(T, wrapper)


class HttpClient:
    def __init__(
            self,
            meta_url: str,
            app_id: str | None = None,
            app_secret: str | None = None,
            timeout: int = 10,
    ):
        self.meta_url = meta_url
        self.app_id = app_id
        self.app_secret = app_secret
        self.timeout = timeout

    def _sign_string(self, string_to_sign: str, secret: str) -> str:
        """签名字符串"""
        signature = hmac.new(
            secret.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha1
        ).digest()
        return base64.b64encode(signature).decode("utf-8")

    def _url_to_path_with_query(self, url: str) -> str:
        """将 URL 转换为带查询参数的路径"""
        parsed = urlparse(url)
        path = parsed.path or "/"
        query = f"?{parsed.query}" if parsed.query else ""
        return path + query

    def _build_http_headers(self, url: str) -> Dict[str, str]:
        """构建 HTTP 请求头"""
        if not self.app_secret:
            return {}

        timestamp = str(int(time.time() * 1000))
        path_with_query = self._url_to_path_with_query(url)
        string_to_sign = f"{timestamp}\n{path_with_query}"
        signature = self._sign_string(string_to_sign, self.app_secret)

        AUTHORIZATION_FORMAT = "Apollo {}:{}"
        HTTP_HEADER_AUTHORIZATION = "Authorization"
        HTTP_HEADER_TIMESTAMP = "Timestamp"

        return {
            HTTP_HEADER_AUTHORIZATION: AUTHORIZATION_FORMAT.format(self.app_id, signature),
            HTTP_HEADER_TIMESTAMP: timestamp,
        }

    def request(
            self,
            method: str,
            path: str,
            params: Optional[Dict[str, Any]] = None,
            data: Optional[Dict[str, Any]] = None,
            json_data: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """发送 HTTP 请求"""
        url = f"{self.meta_url}{path}"
        headers = self._build_http_headers(url)

        logger.debug(f"发送 {method} 请求到 {url}")
        logger.debug(f"请求参数: {params}")
        logger.debug(f"请求头: {headers}")

        response = requests.request(
            method=method,
            url=url,
            params=params,
            data=data,
            json=json_data,
            headers=headers,
            timeout=self.timeout,
        )

        logger.debug(f"响应状态码: {response.status_code}")
        logger.debug(f"响应内容: {response.text}")

        return response

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        """发送 GET 请求"""
        return self.request("GET", path, params=params)

    def post(
            self,
            path: str,
            params: Optional[Dict[str, Any]] = None,
            data: Optional[Dict[str, Any]] = None,
            json_data: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """发送 POST 请求"""
        return self.request("POST", path, params=params, data=data, json_data=json_data)

    def put(
            self,
            path: str,
            params: Optional[Dict[str, Any]] = None,
            data: Optional[Dict[str, Any]] = None,
            json_data: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """发送 PUT 请求"""
        return self.request("PUT", path, params=params, data=data, json_data=json_data)

    def delete(
            self,
            path: str,
            params: Optional[Dict[str, Any]] = None,
            data: Optional[Dict[str, Any]] = None,
            json_data: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """发送 DELETE 请求"""
        return self.request("DELETE", path, params=params, data=data, json_data=json_data)
