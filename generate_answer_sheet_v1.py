
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from reportlab.lib.colors import black, white, HexColor
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

PT_PER_INCH = 72.0
DPI = 300
PAGE_W_PT, PAGE_H_PT = letter
PAGE_W_PX = int(round(PAGE_W_PT / PT_PER_INCH * DPI))
PAGE_H_PX = int(round(PAGE_H_PT / PT_PER_INCH * DPI))

OUT_DIR = Path(__file__).resolve().parent
PDF_PATH = OUT_DIR / "hoja_respuestas_v1_carta.pdf"
JSON_PATH = OUT_DIR / "hoja_respuestas_v1_layout.json"

ACCENT = HexColor("#1f2937")
MID = HexColor("#6b7280")


def px(v_pt: float) -> float:
    return v_pt / PT_PER_INCH * DPI


def to_canvas_y(top_y_pt: float) -> float:
    return PAGE_H_PT - top_y_pt


def draw_text(
    c: canvas.Canvas,
    x_pt: float,
    top_y_pt: float,
    text: str,
    size: float = 10,
    font: str = "Helvetica",
    color=black,
    centered: bool = False,
) -> None:
    c.setFillColor(color)
    c.setFont(font, size)
    y = to_canvas_y(top_y_pt) - size
    if centered:
        c.drawCentredString(x_pt, y, text)
    else:
        c.drawString(x_pt, y, text)


def draw_circle(
    c: canvas.Canvas,
    x_center_pt: float,
    y_center_top_pt: float,
    radius_pt: float,
    line_width: float = 0.8,
) -> None:
    c.setStrokeColor(black)
    c.setLineWidth(line_width)
    c.circle(x_center_pt, to_canvas_y(y_center_top_pt), radius_pt, stroke=1, fill=0)


def draw_marker(c: canvas.Canvas, x_pt: float, top_y_pt: float, size_pt: float) -> Dict[str, float]:
    c.setFillColor(black)
    c.setStrokeColor(black)
    c.rect(x_pt, to_canvas_y(top_y_pt + size_pt), size_pt, size_pt, fill=1, stroke=1)
    return {
        "x_px": px(x_pt + size_pt / 2.0),
        "y_px": px(top_y_pt + size_pt / 2.0),
        "size_px": px(size_pt),
    }


def bubble_spec(x_pt: float, y_top_pt: float, radius_pt: float) -> Dict[str, float]:
    return {"x_px": px(x_pt), "y_px": px(y_top_pt), "radius_px": px(radius_pt)}


def build_answer_sheet() -> None:
    c = canvas.Canvas(str(PDF_PATH), pagesize=letter)
    c.setTitle("Hoja de Respuestas v1")

    marker_size = 22.0
    marker_margin = 18.0
    markers = {
        "top_left": draw_marker(c, marker_margin, marker_margin, marker_size),
        "top_right": draw_marker(c, PAGE_W_PT - marker_margin - marker_size, marker_margin, marker_size),
        "bottom_left": draw_marker(c, marker_margin, PAGE_H_PT - marker_margin - marker_size, marker_size),
        "bottom_right": draw_marker(
            c,
            PAGE_W_PT - marker_margin - marker_size,
            PAGE_H_PT - marker_margin - marker_size,
            marker_size,
        ),
    }

    draw_text(c, PAGE_W_PT / 2.0, 34, "HOJA DE RESPUESTAS", size=17, font="Helvetica-Bold", centered=True)
    draw_text(
        c,
        PAGE_W_PT / 2.0,
        54,
        "Use lápiz pasta oscuro y rellene completamente cada círculo. No marque los cuadrados negros.",
        size=9.4,
        centered=True,
        color=ACCENT,
    )

    left_x = 34
    left_y = 86
    left_w = 392
    left_h = 626
    right_x = 442
    right_y = 86
    right_w = 148
    right_h = 626

    c.setStrokeColor(HexColor("#444444"))
    c.setLineWidth(1.0)
    c.roundRect(left_x, to_canvas_y(left_y + left_h), left_w, left_h, 14, stroke=1, fill=0)
    c.roundRect(right_x, to_canvas_y(right_y + right_h), right_w, right_h, 14, stroke=1, fill=0)

    draw_text(c, left_x + 12, 98, "RESPUESTAS", size=10.5, font="Helvetica-Bold", color=ACCENT)
    draw_text(c, right_x + 12, 98, "IDENTIFICACIÓN", size=10.5, font="Helvetica-Bold", color=ACCENT)

    answers: List[Dict[str, object]] = []
    choice_labels = ["A", "B", "C", "D", "E"]
    group_x = [64, 193, 322]
    row_y0 = 136
    row_pitch = 26
    bubble_pitch_x = 16.5
    bubble_r = 5.2
    number_width = 18

    for block_idx, base_q in enumerate([1, 21, 41]):
        x0 = group_x[block_idx]
        for i, label in enumerate(choice_labels):
            draw_text(c, x0 + number_width + i * bubble_pitch_x - 1.5, 122, label, size=8.2, centered=True, color=ACCENT)
        #c.setStrokeColor(HexColor("#c7c7c7"))
        #c.setLineWidth(0.6)
        #y_line = to_canvas_y(126)
        #c.line(x0 + 2, y_line, x0 + 104, y_line)

        for offset in range(20):
            q = base_q + offset
            y = row_y0 + offset * row_pitch
            draw_text(c, x0, y, f"{q:02d}", size=8.6, font="Helvetica", color=ACCENT)
            choices: Dict[str, Dict[str, float]] = {}
            for ci, ch in enumerate(choice_labels):
                cx = x0 + number_width + ci * bubble_pitch_x
                draw_circle(c, cx, y + 4.5, bubble_r, line_width=0.8)
                choices[ch] = bubble_spec(cx, y + 4.5, bubble_r)
            answers.append({"question": q, "choices": choices})

    rut_columns: List[Dict[str, object]] = []
    rut_x0 = 466
    rut_y0 = 168
    rut_row_pitch = 18.8
    rut_col_pitch = 13.4
    rut_r = 4.9
    row_labels = [str(i) for i in range(10)] + ["K"]
    col_labels = [str(i) for i in range(1, 9)] + ["DV"]

    draw_text(c, right_x + right_w / 2 - 8, 126, "RUT", size=9.4, color=MID)
    c.setStrokeColor(HexColor("#c7c7c7"))
    c.setLineWidth(0.6)
    c.line(right_x + 10, to_canvas_y(140), right_x + right_w - 10, to_canvas_y(140))

    box_size = 13
    box_top_y = 148.0

    digit_centers = [rut_x0 + i * rut_col_pitch for i in range(8)]
    dv_center = rut_x0 + 8 * rut_col_pitch
    c.setStrokeColor(HexColor("#4b5563"))
    c.setStrokeColor(HexColor("#4b5563"))
    for cx in digit_centers:
        c.rect(cx - box_size / 2, to_canvas_y(box_top_y + box_size), box_size, box_size, stroke=1, fill=0)
    
    dash_x = (digit_centers[-1] + dv_center) / 2.0
    draw_text(c, dash_x, box_top_y + 0.6, "-", size=10.5, font="Helvetica-Bold", centered=True, color=MID)
    c.rect(dv_center - box_size / 2, to_canvas_y(box_top_y + box_size), box_size, box_size, stroke=1, fill=0)

    for ri, row_lab in enumerate(row_labels):
        draw_text(c, rut_x0 - 10, rut_y0 + ri * rut_row_pitch - 1, row_lab, size=8.2, centered=True, color=ACCENT)

    for ci, col_lab in enumerate(col_labels):
        col_x = rut_x0 + ci * rut_col_pitch
        options: Dict[str, Dict[str, float]] = {}
        allowed = row_labels if col_lab == "DV" else row_labels[:-1]
        for ri, row_lab in enumerate(row_labels):
            if row_lab not in allowed:
                continue
            cy = rut_y0 + ri * rut_row_pitch + 4.0
            draw_circle(c, col_x, cy, rut_r, line_width=0.75)
            options[row_lab] = bubble_spec(col_x, cy, rut_r)
        rut_columns.append({"position": col_lab, "options": options})

    info_y = 420
    c.setStrokeColor(HexColor("#9a9a9a"))
    c.setLineWidth(0.7)
    for idx, lab in enumerate(["Nombre", "Apellidos", "Curso", "Fecha"]):
        label_y = info_y + 6 + idx * 46
        line_y = label_y + 34
        draw_text(c, right_x + 12, label_y, lab, size=8.5, font="Helvetica-Bold", color=ACCENT)
        c.line(right_x + 12, to_canvas_y(line_y), right_x + right_w - 12, to_canvas_y(line_y))

    footer_y = PAGE_H_PT - 26
    draw_text(c, PAGE_W_PT - 88, footer_y, "VERSION 1", size=8.5, font="Helvetica-Bold", color=MID)

    c.save()

    layout = {
        "template_name": "hoja_respuestas_v1_carta",
        "version": 1,
        "page": {
            "size": "letter",
            "width_pt": PAGE_W_PT,
            "height_pt": PAGE_H_PT,
            "width_px": PAGE_W_PX,
            "height_px": PAGE_H_PX,
            "dpi": DPI,
        },
        "registration_markers": markers,
        "answers": answers,
        "rut": {
            "columns": rut_columns,
            "expected_digits": 8,
            "labels": {
                "ok": "OK",
                "incomplete": "RI",
                "multiple": "RM",
                "dv_mismatch": "DV",
            },
        },
        "response_labels": {
            "blank": "N",
            "double": "RD",
        },
        "reader_defaults": {
            "answer_mark_threshold": 0.28,
            "rut_mark_threshold": 0.26,
            "marker_area_ratio_min": 0.0007,
        },
    }
    JSON_PATH.write_text(json.dumps(layout, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    build_answer_sheet()
