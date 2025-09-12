import unittest

from toollib.log import init_logger

from docsloader import CsvLoader

logger = init_logger(__name__)


class TestCsvLoader(unittest.IsolatedAsyncioTestCase):

    @classmethod
    def setUpClass(cls):
        cls.path_or_url = r"E:\NewFolder\测试.csv"

    async def asyncSetUp(self):
        self.loader = CsvLoader(
            path_or_url=self.path_or_url,
            rm_tmpfile=False,
        )

    async def test_load(self):
        async for item in self.loader.load():
            logger.info(item)


if __name__ == "__main__":
    unittest.main()
