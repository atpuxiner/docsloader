import unittest

from toollib.log import init_logger

from docsloader import PptxLoader

logger = init_logger()


class TestPptxLoader(unittest.IsolatedAsyncioTestCase):

    @classmethod
    def setUpClass(cls):
        cls.path_or_url = r"E:\NewFolder\测试.pptx"

    async def asyncSetUp(self):
        self.loader = PptxLoader(
            path_or_url=self.path_or_url,
            is_rm_tmpfile=False,
        )

    async def test_load(self):
        async for item in self.loader.load():
            logger.info(item)


if __name__ == "__main__":
    unittest.main()
