import unittest

from toollib.log import init_logger

from docsloader import DocxLoader

logger = init_logger(__name__)


class TesDocxLoader(unittest.IsolatedAsyncioTestCase):

    @classmethod
    def setUpClass(cls):
        cls.path_or_url = r"E:\NewFolder\测试.docx"

    async def asyncSetUp(self):
        self.loader = DocxLoader(
            path_or_url=self.path_or_url,
            rm_tmpfile=False,
        )

    async def test_load(self):
        async for doc in self.loader.load():
            logger.info(doc)


if __name__ == "__main__":
    unittest.main()
