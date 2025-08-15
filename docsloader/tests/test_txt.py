import unittest

from toollib.log import init_logger

from docsloader.txt import TxtLoader

logger = init_logger()


class TestTxtLoader(unittest.IsolatedAsyncioTestCase):

    @classmethod
    def setUpClass(cls):
        cls.path_or_url = "https://raw.githubusercontent.com/python/cpython/main/README.rst"

    async def asyncSetUp(self):
        self.loader = TxtLoader(
            path_or_url=self.path_or_url,
        )

    async def test_load(self):
        async for item in self.loader.load():
            logger.info(item)


if __name__ == "__main__":
    unittest.main()
