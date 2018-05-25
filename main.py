r""" _                                    _          _____ _ 
    / \  _   _  __ _ _ __ ___   ___ _ __ | |_ __ _  |_   _(_)
   / _ \| | | |/ _` | '_ ` _ \ / _ \ '_ \| __/ _` |   | | | |
  / ___ \ |_| | (_| | | | | | |  __/ | | | || (_| |   | | | |
 /_/   \_\__,_|\__, |_| |_| |_|\___|_| |_|\__\__,_|   |_| |_|
               |___/                                         
"""

import json
import base64
import asyncio

import aiohttp


class IPFSClient(object):
    """Simple IPFS client that uses asyncio and exposes a convenient interface
    to the experimental pubsub functionality.
    """

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.base = f'http://{self.host}:{self.port}/api/v0'

        self.session = aiohttp.ClientSession()

    async def close(self):
        await self.session.close()

    async def publish(self, topic, data):
        text = json.dumps(data)
        await self._request('/pubsub/pub', topic, text)

    async def subscribe(self, topic):
        async with self._request('/pubsub/sub', topic) as response:
            while True:
                line = await response.content.readline()
                msg = self._parse(line)
                if len(msg) == 0:
                    continue
                yield msg

    def _request(self, url, *args, **kwargs):
        return self.session.get(url=f'{self.base}{url}',
                                params=[('arg', a) for a in args])

    def _parse(self, text):
        msg = json.loads(text)
        if 'data' in msg:
            data = base64.b64decode(msg['data'])
            msg['data'] = json.loads(data)
        return msg


class Application(object):
    
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.client = None

    async def initialize(self):
        self.client = IPFSClient('127.0.0.1', 5001)

    async def shutdown(self):
        await self.client.close()

    async def receive(self):
        print('Receiving messages...')
        async for msg in self.client.subscribe(topic='augmt-12345678'):
            print('  -> received:', msg['data'])

    async def run(self):
        await self.initialize()
        t = asyncio.ensure_future(self.receive())

        for i in range(10):
            await asyncio.sleep(2.0)
            print(f'  <- sending... {i}')
            await self.client.publish(topic='augmt-12345678', data=[1, 2, 3, 4])

        print('Stopping tasks...')
        t.cancel()
        await asyncio.wait([t])

        await self.shutdown()

    def main(self):
        self.loop.run_until_complete(app.run())


if __name__ == "__main__":
    app = Application()
    app.main()
