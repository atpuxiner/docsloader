import unittest

from toollib.log import init_logger

from docsloader import ImgLoader

logger = init_logger(__name__)


class TestImgLoader(unittest.IsolatedAsyncioTestCase):

    @classmethod
    def setUpClass(cls):
        cls.path_or_url = r"E:\NewFolder\测试.png"

    async def asyncSetUp(self):
        self.loader = ImgLoader(
            path_or_url=self.path_or_url,
            rm_tmpfile=False,
        )

    async def test_load(self):
        async for item in self.loader.load():
            logger.info(item)


if __name__ == "__main__":
    unittest.main()
