from playwright.async_api import async_playwright
from TikTokApi import TikTokApi

class DyTikTokApi(TikTokApi):
    def __init__(self):
        super().__init__()

    async def create_chromium(
        self,
        headless=False,
        override_browser_args: list[dict] = None,
    ):
        self.playwright = await async_playwright().start()
        if headless and override_browser_args is None:
            override_browser_args = ["--headless=new"]
            headless = False
        self.browser = await self.playwright.chromium.launch(
            headless=headless,
            args=override_browser_args,
        )

        self.num_sessions = len(self.sessions)

    async def create_session(
        self,
        proxy: str = None,
        sleep_after=3,
        starting_url="https://www.tiktok.com",
    ):
        await super().__create_session(
            proxy=proxy,
            url=starting_url,
            sleep_after=sleep_after
        )

