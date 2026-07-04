#!/usr/bin/env python3
"""Generate the binary sample files for the n1k1 data-source examples:

  - H (compression):  archive/default/orders/*.jsonl.gz
  - L (extract):       kb/default/docs/{handbook.pdf, q1-report.docx, budget.xlsx,
                       deck.pptx}  and  kb/default/media/{logo.png, clip.mp4}

The JSON / JSONL sample files (scenarios A, B, C, E) and the plain-text extract
samples (notes.txt, readme.md, memo.rtf) are checked in directly; only the
binaries are generated here so they're reproducible. Run:

    python3 examples/generate_binaries.py

All files are hand-built minimal-but-valid (no third-party deps): the Office/PDF
files open in Word/Excel/PowerPoint/LibreOffice/any PDF viewer, the PNG is a
valid image, and the MP4 is a valid ISO base-media container.
"""

import gzip
import os
import struct
import zipfile
import zlib

HERE = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------- H: gzip JSONL
def gen_gzip():
    root = os.path.join(HERE, "archive", "default", "orders")
    os.makedirs(root, exist_ok=True)
    datasets = {
        "2025.jsonl.gz": [
            '{"id":"9001","total":75.00,"ts":"2025-06-01"}',
            '{"id":"9002","total":120.00,"ts":"2025-06-15"}',
            '{"id":"9003","total":42.25,"ts":"2025-11-30"}',
        ],
        "2026.jsonl.gz": [
            '{"id":"9101","total":310.00,"ts":"2026-01-08"}',
            '{"id":"9102","total":18.99,"ts":"2026-03-22"}',
        ],
    }
    for name, lines in datasets.items():
        payload = ("\n".join(lines) + "\n").encode("utf-8")
        with gzip.open(os.path.join(root, name), "wb") as f:
            f.write(payload)
        print("wrote", os.path.relpath(os.path.join(root, name), HERE))


# ------------------------------------------------------------------- L: PDF
def gen_pdf():
    root = os.path.join(HERE, "kb", "default", "docs")
    os.makedirs(root, exist_ok=True)

    # A minimal one-page text PDF, with the cross-reference offsets computed
    # programmatically (that's the fiddly part to get right by hand).
    lines = [
        "ACME Employee Handbook",
        "",
        "Vacation Policy: Employees accrue 15 vacation days per year,",
        "prorated in the first year of employment. Unused vacation may",
        "carry over up to 5 days into the following year.",
    ]
    # Build the page content stream (BT/ET text block).
    content = ["BT", "/F1 14 Tf", "72 720 Td", "16 TL"]
    for i, ln in enumerate(lines):
        esc = ln.replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")
        content.append(f"({esc}) Tj" if i == 0 else f"T* ({esc}) Tj")
    content.append("ET")
    stream = "\n".join(content).encode("latin-1")

    objs = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n" % len(stream) + stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]

    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for i, body in enumerate(objs, start=1):
        offsets.append(len(out))
        out += b"%d 0 obj\n" % i + body + b"\nendobj\n"
    xref_pos = len(out)
    out += b"xref\n0 %d\n" % (len(objs) + 1)
    out += b"0000000000 65535 f \n"
    for off in offsets:
        out += b"%010d 00000 n \n" % off
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\n" % (len(objs) + 1)
    out += b"startxref\n%d\n%%%%EOF\n" % xref_pos

    path = os.path.join(root, "handbook.pdf")
    with open(path, "wb") as f:
        f.write(out)
    print("wrote", os.path.relpath(path, HERE))


# ------------------------------------------------------------------- L: DOCX
def gen_docx():
    root = os.path.join(HERE, "kb", "default", "docs")
    os.makedirs(root, exist_ok=True)
    paras = [
        "Q1 2026 Report",
        "Revenue grew 12% quarter over quarter, led by the west region.",
        "Headcount held flat; vacation utilization rose to 68%.",
    ]
    body = "".join(
        "<w:p><w:r><w:t xml:space=\"preserve\">%s</w:t></w:r></w:p>" % p
        for p in paras
    )
    document = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        "<w:body>" + body + "</w:body></w:document>"
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/word/document.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
        "</Types>"
    )
    rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="word/document.xml"/></Relationships>'
    )
    path = os.path.join(root, "q1-report.docx")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", content_types)
        z.writestr("_rels/.rels", rels)
        z.writestr("word/document.xml", document)
    print("wrote", os.path.relpath(path, HERE))


# ------------------------------------------------------------------- L: XLSX
def gen_xlsx():
    root = os.path.join(HERE, "kb", "default", "docs")
    os.makedirs(root, exist_ok=True)
    rows = [
        ["Category", "Q1", "Q2"],
        ["Salaries", "420000", "430000"],
        ["Travel", "18000", "22000"],
        ["Vacation payout", "9500", "10250"],
    ]

    def cell(col, r, val):
        ref = "%s%d" % (chr(ord("A") + col), r)
        # inline string; numbers stored as text keeps the writer dependency-free
        v = val.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return ('<c r="%s" t="inlineStr"><is><t>%s</t></is></c>' % (ref, v))

    sheet_rows = ""
    for r, row in enumerate(rows, start=1):
        cells = "".join(cell(c, r, v) for c, v in enumerate(row))
        sheet_rows += '<row r="%d">%s</row>' % (r, cells)
    sheet = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        "<sheetData>" + sheet_rows + "</sheetData></worksheet>"
    )
    workbook = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="Budget" sheetId="1" r:id="rId1"/></sheets></workbook>'
    )
    wb_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/></Relationships>'
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        "</Types>"
    )
    rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/></Relationships>'
    )
    path = os.path.join(root, "budget.xlsx")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", content_types)
        z.writestr("_rels/.rels", rels)
        z.writestr("xl/workbook.xml", workbook)
        z.writestr("xl/_rels/workbook.xml.rels", wb_rels)
        z.writestr("xl/worksheets/sheet1.xml", sheet)
    print("wrote", os.path.relpath(path, HERE))


# ------------------------------------------------------------------- L: PPTX
def gen_pptx():
    root = os.path.join(HERE, "kb", "default", "docs")
    os.makedirs(root, exist_ok=True)
    slides_text = [
        "Company All-Hands",
        "Q1 revenue grew 12% quarter over quarter.",
        "Reminder: use the HR portal for vacation requests.",
    ]
    A = "http://schemas.openxmlformats.org/drawingml/2006/main"
    P = "http://schemas.openxmlformats.org/presentationml/2006/main"

    def slide_xml(text):
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<p:sld xmlns:a="%s" xmlns:p="%s">'
            "<p:cSld><p:spTree>"
            "<p:sp><p:txBody>"
            "<a:p><a:r><a:t>%s</a:t></a:r></a:p>"
            "</p:txBody></p:sp>"
            "</p:spTree></p:cSld></p:sld>" % (A, P, text)
        )

    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/ppt/presentation.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml"/>'
        "</Types>"
    )
    rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="ppt/presentation.xml"/></Relationships>'
    )
    presentation = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<p:presentation xmlns:p="%s"/>' % P
    )
    path = os.path.join(root, "deck.pptx")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", content_types)
        z.writestr("_rels/.rels", rels)
        z.writestr("ppt/presentation.xml", presentation)
        for i, text in enumerate(slides_text, start=1):
            z.writestr("ppt/slides/slide%d.xml" % i, slide_xml(text))
    print("wrote", os.path.relpath(path, HERE))


# ------------------------------------------------------------------- L: PNG
def gen_png():
    root = os.path.join(HERE, "kb", "default", "media")
    os.makedirs(root, exist_ok=True)
    w, h = 48, 24  # a solid teal rectangle; dimensions are what `extract` reports

    def chunk(tag, data):
        return (
            struct.pack(">I", len(data))
            + tag
            + data
            + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
        )

    ihdr = struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0)  # 8-bit RGB
    raw = bytearray()
    for _ in range(h):
        raw.append(0)  # filter byte per scanline
        raw += bytes((0x1B, 0x9E, 0x8A)) * w
    idat = zlib.compress(bytes(raw), 9)
    png = b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr) + chunk(b"IDAT", idat) + chunk(b"IEND", b"")
    path = os.path.join(root, "logo.png")
    with open(path, "wb") as f:
        f.write(png)
    print("wrote", os.path.relpath(path, HERE))


# ------------------------------------------------------------------- L: MP4
def gen_mp4():
    root = os.path.join(HERE, "kb", "default", "media")
    os.makedirs(root, exist_ok=True)

    def box(tag, payload):
        return struct.pack(">I", 8 + len(payload)) + tag + payload

    # mvhd v0: creation(1904 epoch), timescale, duration -> 4.0s.
    creation = 1577836800 + 2082844800  # 2020-01-01 UTC in seconds since 1904
    mvhd = (
        struct.pack(">I", 0)  # version+flags
        + struct.pack(">I", creation)
        + struct.pack(">I", creation)
        + struct.pack(">I", 1000)  # timescale
        + struct.pack(">I", 4000)  # duration -> 4.0s
        + b"\x00\x01\x00\x00"  # rate 1.0
        + b"\x01\x00"  # volume 1.0
        + b"\x00" * 10  # reserved
        + b"\x00" * 36  # unity matrix (zeros fine for our metadata reader)
        + b"\x00" * 24  # predefined
        + struct.pack(">I", 2)  # next track id
    )
    # tkhd v0: display width/height are the final two 16.16 fixed-point words.
    tkhd = (
        struct.pack(">I", 0)  # version+flags
        + struct.pack(">II", creation, creation)
        + struct.pack(">I", 1)  # track id
        + b"\x00" * 4  # reserved
        + struct.pack(">I", 4000)  # duration
        + b"\x00" * 8  # reserved
        + b"\x00" * 4  # layer + alt group
        + b"\x00" * 4  # volume + reserved
        + b"\x00" * 36  # matrix
        + struct.pack(">I", 640 << 16)  # width
        + struct.pack(">I", 360 << 16)  # height
    )
    moov = box(b"moov", box(b"mvhd", mvhd) + box(b"trak", box(b"tkhd", tkhd)))
    ftyp = box(b"ftyp", b"isom\x00\x00\x02\x00isomiso2mp41")
    path = os.path.join(root, "clip.mp4")
    with open(path, "wb") as f:
        f.write(ftyp + moov)
    print("wrote", os.path.relpath(path, HERE))


if __name__ == "__main__":
    gen_gzip()
    gen_pdf()
    gen_docx()
    gen_xlsx()
    gen_pptx()
    gen_png()
    gen_mp4()
    print("done")
