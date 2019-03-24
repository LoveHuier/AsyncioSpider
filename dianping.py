mport asyncio
import re

import aiohttp
import aiomysql
from pyquery import PyQuery

stopping = False

start_url = "http://www.dianping.com/chengdu/"
waitting_urls = []
seen_urls = set()  # 已爬的url

sem = asyncio.Semaphore(3)  # 设置并发度


async def fetch(url, session):
    """
    获取html
    :param url: 待爬url
    :param session:
    :return: data
    """
    async with sem:
        await asyncio.sleep(1)
        try:
            async with session.get(url) as resp:
                print("url status: {}".format(resp.status))
                if resp.status in [200, 201]:
                    data = await resp.text()  # 异步获取数据
                    # print(data)
                    return data
        except Exception as e:
            print(e)


def extract_urls(html):
    """
    抓取文章详情中的url，并向waitting_urls中添加url
    :param html: 网页内容
    :return:
    """
    urls = []
    pq = PyQuery(html)
    for link in pq.items("a"):  # 利用PyQuery提取所有a的链接
        url = link.attr("href")
        if url and url.startswith("http") and url not in seen_urls:
            urls.append(url)
            waitting_urls.append(url)
            # print("extract_html done")
            # return urls


async def init_urls(url, session):
    """
    获取url页面里的所有url
    :param url:
    :param session:
    :return: waitting_urls
    """
    html = await fetch(url, session)
    seen_urls.add(url)
    waitting_urls = extract_urls(html)
    return waitting_urls


async def article_handler(url, session, pool):
    """
    获取文章详情并解析入库
    :param url:
    :param session:
    :param pool: 连接池
    :return:
    """
    html = await fetch(url, session)
    extract_urls(html)

    seen_urls.add(url)

    pq = PyQuery(html)
    try:
        shop_name = pq.attr('h1').text()
    except TypeError as e:
        pass
    print(shop_name)

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 42;")
            insert_sql = "insert into article_test(shop_name) values('{}')".format(shop_name)
            await cur.execute(insert_sql)


async def consumer(pool):
    """
    不停地从waitting_urls中取出url，并抓取
    :param pool: 连接池
    :return:
    """
    async with aiohttp.ClientSession() as session:
        while not stopping:
            if len(waitting_urls) == 0:
                await asyncio.sleep(0.5)
                continue
            url = waitting_urls.pop()
            print("start get url: {}".format(url))

            # 取得url后，往asyncio中扔协程
            if re.match('http://dianping.com/\d+/', url):  # 判断是否是商店的url
                if url not in seen_urls:
                    # 把协程注入loop中
                    asyncio.ensure_future(article_handler(url, session, pool))
                    asyncio.sleep(1)
                else:
                    if url not in seen_urls:
                        asyncio.ensure_future(init_urls(url, session))


async def main(loop):
    """

    :param loop:事件循环
    :return:
    """
    # 创建连接池,等待mysql连接好
    pool = await aiomysql.create_pool(host='127.0.0.1', port=3306,
                                      user='root', password='ts123456',
                                      db='dianping_mysql', loop=loop,
                                      autocommit=True)

    async with aiohttp.ClientSession() as session:
        html = await fetch(start_url, session)
        seen_urls.add(start_url)
        extract_urls(html)

    asyncio.ensure_future(consumer(pool))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(main(loop))
    # 启动事件循环
    loop.run_forever()

