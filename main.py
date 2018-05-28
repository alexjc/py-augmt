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

    def __init__(self, host='localhost', port=5001, base='api/v0'):
        self.base = f'http://{host}:{port}/{base}'
        self.session = aiohttp.ClientSession()

    async def close(self):
        await self.session.close()

    async def publish(self, topic, data):
        text = json.dumps(data)
        await self._get('/pubsub/pub', topic, text)

    async def subscribe(self, topic):
        async with self._get('/pubsub/sub', topic) as response:
            while True:
                line = await response.content.readline()
                msg = self._parse(line)
                if len(msg) == 0:
                    continue
                yield msg

    async def cat(self, multihash, **kwargs):
        response = await self._get('/cat', multihash)
        return await response.read()

    async def ls(self, multihash, **kwargs):
        return await self._request('/ls', multihash)

    async def refs_local(self, **kwargs):
        return await self._request('/refs/local')

    def _get(self, url, *args, **kwargs):
        return self.session.get(url=f'{self.base}{url}',
                                params=[('arg', a) for a in args])

    async def _request(self, url, *args, **kwargs):
        output = []
        async with self._get(url, *args, **kwargs) as response:
            while True:
                data = await response.content.readline()
                if len(data) == 0:
                    break
                output.append(json.loads(data))
        return output

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
        self.client = None

    async def receive(self):
        print('Receiving messages...')
        async for msg in self.client.subscribe(topic='augmt-12345678'):
            print('  -> received:', msg['data'])

    async def main(self):
        await self.initialize()

        print('Local objects...')
        for item in (await self.client.refs_local()):
            print(await self.client.ls(item['Ref']))

        # data = await self.client.cat('QmZTR5bcpQD7cFgTorqxZDYaew1Wqgfbd2ud9QqGPAkK2V')
        # print(data)

        t = asyncio.ensure_future(self.receive())
        for i in range(10):
            await asyncio.sleep(2.0)
            print(f'  <- sending... {i}')
            await self.client.publish(topic='augmt-12345678', data=[1, 2, 3, 4])

        print('Stopping tasks...')
        t.cancel()
        await asyncio.wait([t])

        await self.shutdown()

    def run(self):
        self.loop.run_until_complete(app.main())


if __name__ == "__main__":
    app = Application()
    app.run()
