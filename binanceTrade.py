import aiohttp
import httpx
import asyncio
import time
import logging
import hmac
from hashlib import sha256
from urllib.parse import urlencode
# 设置log级别
logging.basicConfig(level=logging.INFO)


class BinanceTrade(object):
    def __init__(self, **kwargs):
        config = kwargs.get('config', None)
        self.instIdList = kwargs.get('instIdList', None)
        self.loop = kwargs.get('loop', None)
        self.init_success_callback = kwargs.get('init_success_callback', None)
        self.order_rsp_callback = kwargs.get('order_rsp_callback', None)
        self.create_order_rsp_callback = kwargs.get('create_order_rsp_callback', None)

        # 定义私有频道，用于接受订单回报以及下单
        self.ws_add = config['ws_ws_add']
        self.rest_add = config['rest_add']
        self.api = config['apiKey']
        self.secret = config['secretKey']
        self.passwd = config['passwd']
        self.subscribe_user_data_id = ''

        # 当发出ping并且10秒内未收到pong，重连。先定义一个无效任务，用作后面的判断变量
        self.pong_task = self.loop.call_later(10, None)
        self.pong_task.cancel()

    def __del__(self):
        logging.info("Trade Class Destroyed")

    async def refresh_status(self, newList):
        self.instIdList = newList
        self.subscribe_status = {instId: 0 for instId in self.instIdList}

    async def make_signature(self, query_string):
        return hmac.new(self.secret.encode('utf-8'), query_string.encode('utf-8'), digestmod=sha256).hexdigest()

    async def requests_method(self, client, method):
        if method == 'put':
            result = client.put
        elif method == 'post':
            result = client.post
        elif method == 'get':
            result = client.get
        elif method == 'delete':
            result = client.delete
        return result

    async def make_requests(self, header, order_url, body=None, method='post'):
        if body is not None:
            query_string = urlencode(body).replace("%27", "%22")
            url = self.rest_add + f"{order_url}?{query_string}"
        else:
            url = self.rest_add + order_url
        async with httpx.AsyncClient() as client:
            session = await self.requests_method(client, method)
            result = await session(url, headers=header)
        return result

    async def make_requests_sign(self, header, body, order_url, method='post'):
        query_string = urlencode(body).replace("%27", "%22")
        signature = await self.make_signature(query_string)
        url = self.rest_add + f"{order_url}?{query_string}&signature={signature}"
        async with httpx.AsyncClient() as client:
            session = await self.requests_method(client, method)
            result = await session(url, headers=header)
        return result

    async def get_listenKey(self):
        order_url = '/fapi/v1/listenKey'
        header = {
            "Content-Type": "application/json",
            "X-MBX-APIKEY": self.api
        }
        return (await self.make_requests(header, order_url, method='post')).json()['listenKey']

    async def renew_listenKey(self, listenKey):
        await asyncio.sleep(1800)
        put_url = '/fapi/v1/listenKey'
        header = {
            "Content-Type": "application/json",
            "X-MBX-APIKEY": self.api
        }
        body = {
            "listenKey": listenKey
        }
        result = await self.make_requests(header, put_url, method='put', body=body)
        self.loop.create_task(self.renew_listenKey(listenKey))

    async def connect(self):
        # 撤销所有订单
        rsp = await self.cancel_all_orders()
        # 更改账户持仓模式
        rsp = await self.change_mode()
        # 获取listenKey
        listen_key = await self.get_listenKey()
        # 注册任务刷新listenKey
        self.loop.create_task(self.renew_listenKey(listen_key))
        # 初始化session
        self.ws = await aiohttp.ClientSession().ws_connect(self.ws_add + "/" + listen_key)
        # 订阅订单
        # result = await self.subscribe_order(listen_key)
        self.loop.create_task(self.ws_callback())
        
        if not self.ws.closed:
            await self.init_success_callback("Binance")

    async def reconnect(self):
        logging.warning('Reconnect  || Lost Trade Websocket Session, Reconnect')
        await self.ws.close()
        await self.connect()

    async def subscribe_order(self, listen_key):
        try:
            # 一次性订阅所有的user data
            self.subscribe_user_data_id = int(time.time() * 1000)
            info = {
                "method": "SUBSCRIBE",
                "params": [listen_key],
                "id": self.subscribe_user_data_id
            }
            await self.ws.send_json(info)

        except ConnectionResetError:
            await self.loop.create_task(self.reconnect())

    async def unsubscribe_order(self, instIdList=None):
        if instIdList is None:
            args_list = [{"channel": "orders", "instType": "SWAP", "instId": instId} for instId in self.instIdList]
        else:
            args_list = [{"channel": "orders", "instType": "SWAP", "instId": instId} for instId in instIdList]
        info = {
            "op": "unsubscribe",
            "args": args_list
        }
        await self.ws.send_json(info)

    async def subscribe_rsp_callback(self):
        await self.init_success_callback("Binance")

    async def unsubscribe_rsp_callback(self, data):
        logging.info("Trade: " + data['event'] + "  " + str(data['arg']))

    async def change_mode(self):
        order_url = '/fapi/v1/positionSide/dual'
        try:
            header = {
                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8;",
                "X-MBX-APIKEY": self.api
            }
            body = {
                "timestamp": int(time.time() * 1000),
                "dualSidePosition": "true"
                }
            return await self.make_requests_sign(header, body, order_url, method='post')
        except ConnectionResetError:
            await self.loop.create_task(self.reconnect())

    async def create_order(self, order, opId=None):
        """
        order元素为list[dict]，形如
        {
             "side": "BUY",
             "symbol": "DOGEUSDT",
             "positionSide": "LONG",
             "type": "LIMIT",
             "quantity": str(100),
             "price": str(0.13),
             "timeInForce": "GTC"
        }
        """
        order_url = '/fapi/v1/order'
        if opId is None:
            opId = int(time.time() * 1000)
        try:
            header = {
                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8;",
                "X-MBX-APIKEY": self.api
            }
            order.update({"timestamp": opId})
            return await self.make_requests_sign(header, order, order_url, method='post')
        except ConnectionResetError:
            await self.loop.create_task(self.reconnect())

    async def cancel_order(self, order, opId=None):
        """
        instId: 合约名称
        order dict, 形如
            {
                "symbol": "1",
                "orderId": 2
            }
        """
        order_url = '/fapi/v1/order'
        if opId is None:
            opId = int(time.time() * 1000)
        try:
            header = {
                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8;",
                "X-MBX-APIKEY": self.api
            }
            order.update({"timestamp": opId})
            return await self.make_requests_sign(header, order, order_url, method='delete')
        except ConnectionResetError:
            await self.loop.create_task(self.reconnect())

    async def cancel_all_orders(self):
        """
        instId: 合约名称
        order dict, 形如
            {
                "symbol": "1",
            }
        """
        order_url = '/fapi/v1/allOpenOrders'
        try:
            header = {
                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8;",
                "X-MBX-APIKEY": self.api
            }
            body = {
                "timestamp": int(time.time() * 1000),
                "symbol": self.instIdList[0]
                }
            return await self.make_requests_sign(header, body, order_url, method='delete')
        except ConnectionResetError:
            await self.loop.create_task(self.reconnect())

    async def amend_order(self, order, opId=None):
        """
        instId: 合约名称
        order元素为dict，形如
        {
             "side": "BUY",
             "symbol": "DOGEUSDT",
             "positionSide": "LONG",
             "type": "LIMIT",
             "quantity": str(100),
             "price": str(0.13),
             "timeInForce": "GTC",
             "cancelOrderId": 12345,
             "cancelReplaceMode": "STOP_ON_FAILURE"
        }
        """
        order_url = '/fapi/v1/order'
        if opId is None:
            opId = int(time.time() * 1000)
        try:
            header = {
                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8;",
                "X-MBX-APIKEY": self.api
            }
            order.update({"timestamp": opId})
            return await self.make_requests_sign(header, order, order_url, method='put')
        except ConnectionResetError:
            await self.loop.create_task(self.reconnect())

    async def heartbeat(self):
        await self.ws.ping()
        while True:
            logging.info(str(time.time()) + " || " + "Trade ping")
            await asyncio.sleep(300)
            await self.ws.ping()

    async def ws_callback(self):
        heartbeat = self.loop.create_task(self.heartbeat())
        while True:
            msg = await self.ws.receive()
            # 如果断连了，那么msg收到的msg.data会是None，于是我们强制跳出循环，并进行重连
            if msg.data is None:
                break
            else:
                data = msg.json()
                # 发送订阅的回报
                if data.get("e", "") == "ORDER_TRADE_UPDATE":
                    await self.order_rsp_callback(data)
        logging.info("trade callback out")
        heartbeat.cancel()
        # 如果莫名其妙跳出了ws循环，则发一个心跳，让ws重连
        logging.info("Trade module: Step out of websocket loop")
        self.loop.create_task(self.reconnect())


if __name__ == '__main__':
    mkt = BinanceTrade()
