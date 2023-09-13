import asyncio
import aiofiles
import aiohttp
import os, json

from django.http import HttpResponse

from django.http import JsonResponse
from django.conf import settings

HASH_TABLE_FILE_PATH = os.path.join(settings.BASE_DIR, 'proxy', 'data', 'hash_table.json')

async def read_hash_table_from_file(file_path):
    async with aiofiles.open(file_path, mode='r') as f:
        content = await f.read()
        hash_table = json.loads(content)
    return hash_table


async def update_hash_table_to_file(file_path, hash_table):
    async with aiofiles.open(file_path, mode='w') as f:
        content = json.dumps(hash_table)
        await f.write(content)

def get_client_ip_port(request):
    # 获取客户端IP
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')

    # 获取客户端端口
    #port = request.META.get('REMOTE_PORT')
    port = request.environ.get('REMOTE_PORT')

    return ip, port

async def handle_client_request(request):

    backend_servers = [{'ip':'192.168.6.24', 'port':6677}]

    hash_table = await read_hash_table_from_file(HASH_TABLE_FILE_PATH)

    # 获取客户端IP和端口
    client_ip, client_port = get_client_ip_port(request)

    # 先从哈希表中查询是否已有记录，如果有则直接转发到对应服务器，否则需要进行服务端的轮询
    cc = (client_ip, client_port)
    backend_server = hash_table.get(str(cc))
    if backend_server:
        # 直接转发请求到对应的服务器
        response = await try_forward_request(request, backend_server, update=False)
    else:
        # 进行服务端轮询
        for server in backend_servers:
            response = await try_forward_request(request, server)
            if response.status == 404:
                #TODO 判断是否选择此服务端 依据是返回的json中包含一定的状态值

                # 转发请求成功，更新哈希表
                cc = (client_ip, client_port)
                hash_table[str(cc)] = server
                await update_hash_table_to_file(HASH_TABLE_FILE_PATH,hash_table)
                break

    # 将后端服务器返回的响应原样返回给客户端
    content = await response.content.read()
    return HttpResponse(content=content, status=response.status, content_type=response.headers.get('Content-Type', 'text/plain'))

async def try_forward_request(request, server, update=True):
    hash_table = await read_hash_table_from_file(HASH_TABLE_FILE_PATH)
    url = f"http://{server['ip']}:{server['port']}{request.path}"
    async with aiohttp.ClientSession() as session:
        headers = request.headers.copy()
        #headers.pop('Host', None)
        #headers.pop('Content-Length', None)
        async with session.request(request.method, url, headers=headers, data=request.body) as response:
            if response.status == 200:
                if update:
                    # 转发请求成功，更新哈希表
                    hash_table[str(client_ip, client_port)] = server
            return response



#{"('192.168.0.27', 6677)": {'ip': '192.168.8.10', 'port': '9988'}}

