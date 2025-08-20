import logging
from typing import AsyncGenerator, Dict, Any
import zipfile
from pathlib import Path

from docx import Document as DocxDocument
from docx.table import Table
from docx.text.paragraph import Paragraph

from docsloader.base import BaseLoader

logger = logging.getLogger(__name__)


class DocxLoader(BaseLoader):

    async def load_by_basic(self) -> AsyncGenerator[Dict[str, Any], None]:
        try:
            idx = 0
            for item in self.extract_by_python_docx(tmpfile=await self.tmpfile):
                metadata = self.metadata.copy()
                metadata.update({"idx": idx})
                if item['type'] == 'table':
                    yield {
                        "type": "table",
                        "text": item.get("text", ""),
                        "data": item.get("data"),
                        "metadata": metadata,
                    }
                elif item['type'] == 'image':
                    yield {
                        "type": "image",
                        "text": item.get("text", ""),
                        "path": item.get("path"),
                        "metadata": metadata,
                    }
                else:
                    yield {
                        "type": "text",
                        "text": item.get("text", ""),
                        "metadata": metadata,
                    }
                idx += 1
        finally:
            await self.rm_tmpfile()

    @staticmethod
    def extract_by_python_docx(tmpfile: str):
        doc = DocxDocument(tmpfile)
        # media
        image_dir = Path(tmpfile + "_media")
        image_dir.mkdir(parents=True, exist_ok=True)
        image_map = {}  # relId -> local image path
        image_counter = 1
        try:
            with zipfile.ZipFile(tmpfile, 'r') as z:
                for file_info in z.infolist():
                    if file_info.filename.startswith('word/media/'):
                        ext = Path(file_info.filename).suffix
                        local_image_path = image_dir / f"image_{image_counter}{ext}"
                        with open(local_image_path, 'wb') as f:
                            f.write(z.read(file_info.filename))
                        # relId map
                        image_map[file_info.filename] = str(local_image_path)
                        image_counter += 1
        except Exception as e:
            logger.error(f"extracting the image failed: {e}")
        # body
        for element in doc.element.body:
            if element.tag.endswith('p'):
                paragraph = Paragraph(element, doc)
                text = paragraph.text.strip()
                drawing_nodes = element.xpath('.//wp:docPr/parent::wp:anchor|.//wp:docPr/parent::wp:inline')
                images_in_para = []
                for node in drawing_nodes:
                    rel_id = None
                    # 1. a:blip + r:embed
                    blip_nodes = node.xpath('.//a:blip')
                    if blip_nodes:
                        embed_attr = '{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed'
                        embed = blip_nodes[0].get(embed_attr)
                        if embed:
                            rel_id = embed
                    # 2. a:imagedata 或 v:shape/@imagedata + r:id
                    if not rel_id:
                        imagedatas = node.xpath('.//a:imagedata | .//v:imagedata')
                        for imgdata in imagedatas:
                            rid = imgdata.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
                            if rid:
                                rel_id = rid
                                break
                    # 3. v:shape imagedata
                    if not rel_id:
                        v_shape_imagedata = node.xpath('.//v:shape/@imagedata')
                        for attr in v_shape_imagedata:
                            parent = attr.getparent()
                            rid = parent.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
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
                            'type': 'image',
                            'text': '',
                            'path': image_path
                        }
                elif text:
                    yield {
                        'type': 'paragraph',
                        'text': text
                    }
            elif element.tag.endswith('tbl'):
                table = Table(element, doc)
                data = []
                for row in table.rows:
                    row_data = []
                    for cell in row.cells:
                        cell_text = ''.join(p.text for p in cell.paragraphs).strip()
                        row_data.append(cell_text)
                    data.append(row_data)
                yield {
                    'type': 'table',
                    'text': '',
                    'data': data
                }
