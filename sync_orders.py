import asyncio
import websockets
import json

with open('keys.json', 'r') as file:
    client_id_to_uuid = json.load(file)
uuid_to_client_id = {v: k for k, v in client_id_to_uuid.items()}

client_websockets = {}

async def relay_orders(order_data, origin_uuid):
    origin_client_id = uuid_to_client_id[origin_uuid]

    for uuid, info in client_websockets.items():
        if uuid != origin_uuid:
            await info['websocket'].send(json.dumps({
                "new_order": order_data
            }))

async def handler(websocket, path):
    """处理WebSocket连接，接收和转发订单。"""
    uuid = None
    try:
        async for message in websocket:
            data = json.loads(message)
            uuid = data.get('uuid')

            if uuid not in uuid_to_client_id:
                await websocket.send(json.dumps({"error": "Authentication failed"}))
                return

            client_websockets[uuid] = {
                'websocket': websocket
            }

            if data.get('action') == 'new_order':
                await relay_orders(data, uuid)
            elif data.get('action') == 'unregister':
                if uuid in client_websockets:
                    del client_websockets[uuid]
                    await websocket.send(json.dumps({"info": "Client unregistered successfully"}))
                    break
    finally:
        if uuid and uuid in client_websockets:
            del client_websockets[uuid]

async def start_server():
    """启动WebSocket服务器。"""
    server = await websockets.serve(handler, "0.0.0.0", 19201)
    await server.wait_closed()

asyncio.run(start_server())
