import unittest

from toollib.log import init_logger

from docsloader import PdfLoader

logger = init_logger()


class TestPdfLoader(unittest.IsolatedAsyncioTestCase):

    @classmethod
    def setUpClass(cls):
        cls.path_or_url = r"E:\NewFolder\测试.pdf"

    async def asyncSetUp(self):
        self.loader = PdfLoader(
            path_or_url=self.path_or_url,
            rm_tmpfile=False,
            # load_type="pdfplumber"
        )

    async def test_load(self):
        async for item in self.loader.load(
                # pdf_max_workers=None,
                # keep_page_image=True,
                # keep_emdb_image=True,
        ):
            logger.info(item)


if __name__ == "__main__":
    unittest.main()
