import json

import ts3
import configuration
import asyncio
import aioredis

#TODO: write your own exception

def establish_connection():
    ts3conn = ts3.query.TS3Connection(configuration.ip, configuration.port)
    ts3conn.login(client_login_name=configuration.client_login_name, client_login_password=configuration.client_login_password)
    ts3conn.use(sid=configuration.sid)
    ts3conn.clientupdate(client_nickname=configuration.client_nickname)
    return ts3conn


class TS3Connection:
    connection = None

    def __init__(self):
        if not TS3Connection.connection:
            TS3Connection.connection = establish_connection()


async def subscribe():
    conn = TS3Connection().connection
    conn.servernotifyregister(event="textprivate")

async def send_message(clid, msg):
    conn = TS3Connection().connection
    conn.sendtextmessage(targetmode=1, target=clid, msg=msg)


async def get_message():
    #Blocking
    conn = TS3Connection().connection
    event = conn.wait_for_event(timeout=0.1)
    print(event[0])
    return event[0]



############################

async def pop_message_from_redis():
    sub = await aioredis.create_redis(
        'redis://localhost', password='xd')
    message = await sub.blpop('messages')
    x = json.loads(message[1].decode('utf-8'))
    await send_message(x["clid"], x["msg"])
    sub.close()


async def pop_message():
    pub = await aioredis.create_redis(
        'redis://localhost')
    res = await pub.publish_json('chan:1', ["Hello", "world"])
    assert res == 1
    pub.close()

async def push_message():
    pub = await aioredis.create_redis(
        'redis://localhost', password='xd')
    message = await get_message()
    if not message:
        return None
    res = await pub.lpush('messages_received', str(message))
    print(res)
    pub.close()
        


async def main():
    while True:
        await subscribe()
        await pop_message_from_redis()
        await push_message()


if __name__ == "__main__":
    print('manemain')
    loop = asyncio.new_event_loop()
    #asyncio.set_event_loop(loop)
    try:
        #loop.run_forever()
        loop.run_until_complete(main())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
