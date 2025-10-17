from typing import Literal


def format_table(
        table: list,
        fmt: Literal["html", "md"] = 'html',
) -> str:
    """format table"""
    if not table:
        return ""
    if not all(isinstance(item, list) for item in table):
        if fmt == 'md':
            return "| " + " | ".join(map(str, table)) + " |"
        else:
            return "<tr>" + "".join(f"<td>{r}</td>" for r in map(str, table)) + "</tr>"
    headers = table[0] if not isinstance(table[0], str) else table
    if fmt == 'md':
        md = "| " + " | ".join(map(str, headers)) + " |\n"
        md += "| " + " | ".join(["---"] * len(headers)) + " |\n"
        for row in table[1:]:
            md += "| " + " | ".join(map(str, row)) + " |\n"
        return md
    else:
        html = "<table>"
        html += "".join(f"<th>{h}</th>" for h in map(str, headers))
        for row in table[1:]:
            html += "<tr>" + "".join(f"<td>{d}</td>" for d in map(str, row)) + "</tr>"
        html += "</table>"
        return html
