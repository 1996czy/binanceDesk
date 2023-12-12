
import aiohttp
import asyncio
import time
import logging
# 设置log级别
logging.basicConfig(level=logging.INFO)


class BinanceMarket(object):
    def __init__(self, **kwargs):
        config = kwargs.get('config', None)
        self.instIdList = kwargs.get('instIdList', None)
        self.trade_callback = kwargs.get('trade_callback', None)
        self.depth_callback = kwargs.get('depth_callback', None)
        self.bookTicker_callback = kwargs.get('bookTicker_callback', None)
        self.kline_callback = kwargs.get('kline_callback', None)
        self.liquidation_callback = kwargs.get('liquidation_callback', None)
        self.init_success_callback = kwargs.get('init_success_callback', None)
        self.loop = kwargs.get('loop', None)
        self.cancel_all = kwargs.get('cancel_all', None)

        # 定义公有频道，用于接收行情数据
        self.ws_add = config['ws_stream_add']

        self.subscribe_id = []
        self.refresh_status = False

        # 当发出ping并且10秒内未收到pong，重连。先定义一个无效任务，用作后面的判断变量
        self.pong_task = self.loop.call_later(10, None)
        self.pong_task.cancel()

    def __del__(self):
        logging.info("Market Class Destroyed")

    async def timer(self):
        await asyncio.sleep(3600 * 23)
        logging.info('REFRESH MARKET WEBSOCKET CONNECTION')
        self.refresh_status = True

    async def connect(self):
        # 初始化websocket
        streams = ""
        if self.trade_callback is not None:
            streams += "/".join([f'{instId.lower()}@aggTrade' for instId in self.instIdList]) + "/"
        if self.depth_callback is not None:
            streams += "/".join([f'{instId.lower()}@depth5' for instId in self.instIdList]) + "/"
        if self.bookTicker_callback is not None:
            streams += "/".join([f'{instId.lower()}@bookTicker' for instId in self.instIdList]) + "/"
        if self.kline_callback is not None:
            streams += "/".join([f'{instId.lower()}@kline_1s' for instId in self.instIdList]) + "/"
        streams = streams[:-1]
        self.ws = await aiohttp.ClientSession().ws_connect(self.ws_add + f"?streams={streams}")
        # 读取websocket返回数据并处理
        self.refresh_status = False
        self.loop.create_task(self.ws_callback())
        self.loop.create_task(self.timer())
        # if self.trade_callback is not None:
        #     await self.subscribe_trades()
        # if self.depth_callback is not None:
        #     await self.subscribe_depth()
        # if self.bookTicker_callback is not None:
        #     await self.subscribe_bookTicker()

    async def reconnect(self):
        logging.info('Reconnect  || Lost Market Websocket Session, Reconnect')
        await self.ws.close()
        # await self.cancel_all()
        await self.connect()

    async def subscribe_bookTicker(self, instIdList=None):
        if instIdList is None:
            args_list = [{"channel": "trades", "instId": instId} for instId in self.instIdList]
        else:
            args_list = [{"channel": "trades", "instId": instId} for instId in instIdList]
        try:
            # 每次发十个Inst，不然会被binance强行断连
            i = 0
            while i < len(args_list):
                ws_id = int(time.time() * 1000)
                self.subscribe_id.append(ws_id)
                await self.ws.send_json({
                    "method": "SUBSCRIBE",
                    "params":
                    [
                        f"{instId}@bookTicker" for instId in args_list[i:i + 10]
                    ],
                    "id": ws_id
                })
                i += 10
                await asyncio.sleep(1)
        except ConnectionResetError:
            await self.loop.create_task(self.reconnect())

    async def subscribe_depth(self, instIdList=None):
        if instIdList is None:
            args_list = [{"channel": "trades", "instId": instId} for instId in self.instIdList]
        else:
            args_list = [{"channel": "trades", "instId": instId} for instId in instIdList]
        try:
            # 每次发十个Inst，不然会被binance强行断连
            i = 0
            while i < len(args_list):
                ws_id = int(time.time() * 1000)
                self.subscribe_id.append(ws_id)
                await self.ws.send_json({
                    "method": "SUBSCRIBE",
                    "params":
                    [
                        f"{instId}@depth5@500ms" for instId in args_list[i:i + 10]
                    ],
                    "id": ws_id
                })
                i += 10
                await asyncio.sleep(1)
        except ConnectionResetError:
            await self.loop.create_task(self.reconnect())

    async def subscribe_trades(self, instIdList=None):
        if instIdList is None:
            args_list = [{"channel": "trades", "instId": instId} for instId in self.instIdList]
        else:
            args_list = [{"channel": "trades", "instId": instId} for instId in instIdList]
        try:
            # 每次发十个Inst，不然会被binance强行断连
            i = 0
            while i < len(args_list):
                ws_id = int(time.time() * 1000)
                self.subscribe_id.append(ws_id)
                await self.ws.send_json({
                    "method": "SUBSCRIBE",
                    "params":
                    [
                        f"{instId}@aggTrade" for instId in args_list[i:i + 10]
                    ],
                    "id": ws_id
                })
                i += 10
                await asyncio.sleep(1)
        except ConnectionResetError:
            await self.loop.create_task(self.reconnect())

    async def unsubscribe_trades(self, instIdList=None):
        if instIdList is None:
            args_list = [{"channel": "trades", "instId": instId} for instId in self.instIdList]
        else:
            args_list = [{"channel": "trades", "instId": instId} for instId in instIdList]
        try:
            # 每次发十个Inst，不然会被binance强行断连
            i = 0
            while i < len(args_list):
                ws_id = int(time.time() * 1000)
                self.subscribe_id.append(ws_id)
                await self.ws.send_json({
                    "method": "UNSUBSCRIBE",
                    "params":
                        [
                            f"{instId}@aggTrade" for instId in args_list[i:i + 10]
                        ],
                    "id": ws_id
                })
                i += 10
                await asyncio.sleep(1)
        except ConnectionResetError:
            await self.loop.create_task(self.reconnect())

    async def subscribe_rsp_callback(self, data):
        self.subscribe_id.remove(data['id'])
        if not self.subscribe_id:
            await self.init_success_callback("Binance")

    async def unsubscribe_rsp_callback(self, data):
        logging.info("Market: " + data['event'] + "  " + str(data['arg']))

    async def heartbeat(self):
        await self.ws.ping()
        while True:
            logging.info(str(time.time()) + " || " + "Market ping")
            await asyncio.sleep(300)
            await self.ws.ping()

    async def ws_callback(self):
        heartbeat = self.loop.create_task(self.heartbeat())
        while not self.refresh_status:
            msg = await self.ws.receive()
            # 如果断连了，那么msg收到的msg.data会是None，于是我们强制跳出循环，并进行重连
            if msg.data is None:
                logging.info("None message, break out the loop")
                break
            else:
                data = msg.json()
                # logging.info(msg.data)
                # 发送订阅的回报
                if data.get("id", "") in self.subscribe_id:
                    await self.subscribe_rsp_callback(data)
                # 成交rtn
                elif "aggTrade" in data.get("stream", ""):
                    await self.trade_callback(data)
                # 深度rtn
                elif "depth" in data.get("stream", ""):
                    await self.depth_callback(data)
                # 逐笔最优rtn
                elif "bookTicker" in data.get("stream", ""):
                    await self.bookTicker_callback(data)
                # 强平订单rtn
                elif "forceOrder" in data.get("stream", ""):
                    await self.liquidation_callback(data)
                # k线rtn
                elif "kline" in data.get("stream", ""):
                    await self.kline_callback(data)
        logging.info("market callback out")
        heartbeat.cancel()
        if not self.refresh_status:
            # 如果莫名其妙跳出了ws循环，则发一个心跳，让ws重连
            logging.info("Market module: Step out of websocket loop")
            self.loop.create_task(self.reconnect())
        else:
            await self.ws.close()
            await self.connect()


if __name__ == '__main__':
    mkt = BinanceMarket()
