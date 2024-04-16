import asyncio
import re
from playwright.async_api import async_playwright
from PyQt6.QtCore import QRunnable

from globals import Globals

class Browser(QRunnable):
    def __init__(self):
        super().__init__()

        self.user = 'ToCaptcha'

        Globals._Log.info(self.user, 'Successfully initialized.')

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.run_playwright())

    async def refresh_token(self, url, username, password):
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            await page.goto(url)



            await browser.close()

async def get_captcha_image_and_uuid():
    async with async_playwright() as p:
        # 启动浏览器（非无头模式）
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        # 访问网页
        await page.goto('https://admin.asia.reelshors.com')

        # 等待验证码图片元素加载
        captcha_selector = '.login-captcha img'  # 使用CSS选择器定位验证码图片
        await page.wait_for_selector(captcha_selector)

        # 获取验证码图片元素
        captcha_element = await page.query_selector(captcha_selector)

        # 获取图片的 src 属性
        captcha_src = await captcha_element.get_attribute('src')
        print(f"Captcha SRC: {captcha_src}")

        # 从 src 中提取 UUID
        uuid_match = re.search(r'uuid=([\w-]+)', captcha_src)
        if uuid_match:
            uuid = uuid_match.group(1)
            print(f"Captcha UUID: {uuid}")

        # 直接从浏览器中获取图片的二进制内容
        captcha_image_data = await captcha_element.screenshot()

        # 保存图片数据到本地文件
        with open('captcha.png', 'wb') as f:
            f.write(captcha_image_data)

        print("验证码图片已保存")

        # 等待一段时间或进行其他操作
        await page.wait_for_timeout(5000)

        # 关闭浏览器
        await browser.close()

asyncio.run(get_captcha_image_and_uuid())
