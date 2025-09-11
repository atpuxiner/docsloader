import logging
import os
import zipfile
from typing import AsyncGenerator

from docx import Document as DocxDocument
from docx.table import Table
from docx.text.paragraph import Paragraph

from docsloader.base import BaseLoader, DocsData
from docsloader.utils import format_table, format_image, office_cvt_openxml

logger = logging.getLogger(__name__)


class DocxLoader(BaseLoader):
    """
    docx loader

    params:
        - path_or_url: str
        - suffix: str = None
        - encoding: str = None
        - load_type: str = "basic"
        - load_options: dict = None
        - metadata: dict = None
        - rm_tmpfile: bool = False
    """

    async def load_by_basic(self) -> AsyncGenerator[DocsData, None]:
        image_fmt = self.load_options.get("image_fmt")
        table_fmt = self.load_options.get("table_fmt")
        tmpfile_cvt = None
        if self.suffix == ".doc":
            tmpfile_cvt = office_cvt_openxml(filepath=self.tmpfile, file_suffix=self.suffix)
        for item in self.extract_by_python_docx(
                filepath=tmpfile_cvt or self.tmpfile,
                image_fmt=image_fmt,
                table_fmt=table_fmt,
        ):
            yield DocsData(
                type=item.get("type"),
                text=item.get("text"),
                data=item.get("data"),
                metadata=self.metadata,
            )
        if tmpfile_cvt:
            self.rm_file(filepath=tmpfile_cvt)

    def extract_by_python_docx(
            self,
            filepath: str,
            image_fmt: str = "path",
            table_fmt: str = "html"
    ) -> dict:
        tmpdir = self.mk_tmpdir()
        image_map = {}  # relId -> local image path
        image_counter = 1
        try:
            with zipfile.ZipFile(filepath, mode="r") as z:
                for file_info in z.infolist():
                    if file_info.filename.startswith("word/media/"):
                        _, ext = os.path.splitext(file_info.filename)
                        image_path = os.path.join(tmpdir, f"image_{image_counter}{ext}")
                        with open(image_path, "wb") as f:
                            f.write(z.read(file_info.filename))
                        # relId map
                        image_map[file_info.filename] = image_path
                        image_counter += 1
            self.rm_empty_dir(tmpdir)
        except Exception as e:
            logger.error(f"extracting the image failed: {e}")
        doc = DocxDocument(filepath)
        for element in doc.element.body:
            if element.tag.endswith("p"):
                paragraph = Paragraph(element, doc)
                para_text = paragraph.text.strip()
                drawing_nodes = element.xpath(".//wp:docPr/parent::wp:anchor|.//wp:docPr/parent::wp:inline")
                images_in_para = []
                for node in drawing_nodes:
                    rel_id = None
                    # 1. a:blip + r:embed
                    blip_nodes = node.xpath(".//a:blip")
                    if blip_nodes:
                        embed_attr = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed"
                        embed = blip_nodes[0].get(embed_attr)
                        if embed:
                            rel_id = embed
                    # 2. a:imagedata 或 v:shape/@imagedata + r:id
                    if not rel_id:
                        imagedata = node.xpath(".//a:imagedata | .//v:imagedata")
                        for imgdata in imagedata:
                            rid = imgdata.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
                            if rid:
                                rel_id = rid
                                break
                    # 3. v:shape imagedata
                    if not rel_id:
                        v_shape_imagedata = node.xpath(".//v:shape/@imagedata")
                        for attr in v_shape_imagedata:
                            parent = attr.getparent()
                            rid = parent.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
                            if rid:
                                rel_id = rid
                                break
                    if rel_id:
                        try:
                            target_ref = doc.part.rels[rel_id].target_ref
                            zip_image_path = f"word/{target_ref}"
                            if zip_image_path in image_map:
                                images_in_para.append(image_map[zip_image_path])
                        except KeyError:
                            logger.error(f"Image not found for relId: {rel_id}")
                if images_in_para:
                    for image_path in images_in_para:
                        yield {
                            "type": "image",
                            "text": format_image(image_path, fmt=image_fmt),  # noqa
                            "data": image_path
                        }
                elif para_text:
                    yield {
                        "type": "text",
                        "text": para_text
                    }
            elif element.tag.endswith("tbl"):
                table = Table(element, doc)
                table_data = []
                for row in table.rows:
                    row_data = []
                    for cell in row.cells:
                        cell_text = "".join(p.text for p in cell.paragraphs).strip()
                        row_data.append(cell_text)
                    table_data.append(row_data)
                yield {
                    "type": "table",
                    "text": format_table(table_data, fmt=table_fmt),  # noqa
                    "data": table_data
                }
