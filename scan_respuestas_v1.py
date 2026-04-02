
from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import cv2
import numpy as np
import pandas as pd


VALID_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp"}


@dataclass
class Bubble:
    label: str
    x: float
    y: float
    radius: float


class MarkerDetectionError(RuntimeError):
    pass


def load_layout(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def list_images(input_dir: Path) -> List[Path]:
    files = [p for p in sorted(input_dir.iterdir()) if p.suffix.lower() in VALID_EXTENSIONS]
    if not files:
        raise FileNotFoundError(
            f"No se encontraron imagenes compatibles en {input_dir}. "
            f"Usa JPG, PNG o TIFF exportados por el scanner."
        )
    return files


def read_image(path: Path) -> np.ndarray:
    image = cv2.imread(str(path), cv2.IMREAD_COLOR)
    if image is None:
        raise ValueError(f"No se pudo abrir la imagen: {path}")
    return image


def to_gray(image: np.ndarray) -> np.ndarray:
    return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)


def detect_markers(gray: np.ndarray, layout: dict) -> Dict[str, Tuple[float, float]]:
    page_cfg = layout["page"]
    img_h, img_w = gray.shape[:2]
    image_area = img_h * img_w
    min_area = image_area * float(layout["reader_defaults"].get("marker_area_ratio_min", 0.0007))

    blur = cv2.GaussianBlur(gray, (5, 5), 0)
    _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    thresh = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, np.ones((5, 5), np.uint8))

    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    candidates: List[Tuple[float, float, float]] = []
    for cnt in contours:
        area = cv2.contourArea(cnt)
        if area < min_area:
            continue
        x, y, w, h = cv2.boundingRect(cnt)
        if min(w, h) == 0:
            continue
        aspect = w / float(h)
        if not (0.75 <= aspect <= 1.25):
            continue
        fill_ratio = area / float(w * h)
        if fill_ratio < 0.78:
            continue
        candidates.append((x + w / 2.0, y + h / 2.0, area))

    if len(candidates) < 4:
        raise MarkerDetectionError(
            f"Solo se detectaron {len(candidates)} candidatos a marcador. "
            "Revisa resolucion, contraste o recorte de la imagen."
        )

    points = np.array([[c[0], c[1]] for c in sorted(candidates, key=lambda t: t[2], reverse=True)[:12]], dtype=np.float32)
    img_corners = {
        "top_left": np.array([0.0, 0.0], dtype=np.float32),
        "top_right": np.array([img_w - 1.0, 0.0], dtype=np.float32),
        "bottom_left": np.array([0.0, img_h - 1.0], dtype=np.float32),
        "bottom_right": np.array([img_w - 1.0, img_h - 1.0], dtype=np.float32),
    }

    picked: Dict[str, np.ndarray] = {}
    remaining = points.copy()
    for name, corner in img_corners.items():
        if len(remaining) == 0:
            break
        dists = np.linalg.norm(remaining - corner, axis=1)
        idx = int(np.argmin(dists))
        picked[name] = remaining[idx]
        remaining = np.delete(remaining, idx, axis=0)

    if len(picked) != 4:
        raise MarkerDetectionError("No fue posible asignar los 4 marcadores a las esquinas.")

    return {name: (float(pt[0]), float(pt[1])) for name, pt in picked.items()}


def warp_to_template(image: np.ndarray, detected_markers: Dict[str, Tuple[float, float]], layout: dict) -> np.ndarray:
    page_cfg = layout["page"]
    w = int(page_cfg["width_px"])
    h = int(page_cfg["height_px"])

    src = np.array(
        [
            detected_markers["top_left"],
            detected_markers["top_right"],
            detected_markers["bottom_left"],
            detected_markers["bottom_right"],
        ],
        dtype=np.float32,
    )

    marker_cfg = layout["registration_markers"]
    dst = np.array(
        [
            (marker_cfg["top_left"]["x_px"], marker_cfg["top_left"]["y_px"]),
            (marker_cfg["top_right"]["x_px"], marker_cfg["top_right"]["y_px"]),
            (marker_cfg["bottom_left"]["x_px"], marker_cfg["bottom_left"]["y_px"]),
            (marker_cfg["bottom_right"]["x_px"], marker_cfg["bottom_right"]["y_px"]),
        ],
        dtype=np.float32,
    )

    transform = cv2.getPerspectiveTransform(src, dst)
    warped = cv2.warpPerspective(image, transform, (w, h), flags=cv2.INTER_LINEAR, borderValue=(255, 255, 255))
    return warped


def binarize(gray: np.ndarray) -> np.ndarray:
    blur = cv2.GaussianBlur(gray, (5, 5), 0)
    _, binary = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    return binary


def circle_fill_ratio(binary_inv: np.ndarray, x: float, y: float, radius: float) -> float:
    r = max(int(round(radius * 0.95)), 1)
    x_i = int(round(x))
    y_i = int(round(y))
    x0 = max(x_i - r - 2, 0)
    y0 = max(y_i - r - 2, 0)
    x1 = min(x_i + r + 3, binary_inv.shape[1])
    y1 = min(y_i + r + 3, binary_inv.shape[0])

    roi = binary_inv[y0:y1, x0:x1]
    mask = np.zeros(roi.shape, dtype=np.uint8)
    cv2.circle(mask, (x_i - x0, y_i - y0), r, 255, thickness=-1)

    pixels = roi[mask == 255]
    if pixels.size == 0:
        return 0.0
    return float(np.count_nonzero(pixels)) / float(pixels.size)


def decode_single_mark(
    ratios: Dict[str, float],
    threshold: float,
    blank_label: str,
    multiple_label: str,
) -> str:
    ordered = sorted(ratios.items(), key=lambda item: item[1], reverse=True)
    best_label, best_value = ordered[0]
    second_value = ordered[1][1] if len(ordered) > 1 else 0.0

    if best_value < threshold:
        return blank_label

    if second_value >= threshold or (second_value >= max(threshold - 0.04, best_value * 0.82)):
        return multiple_label

    return best_label


def decode_answers(binary_inv: np.ndarray, layout: dict, threshold: float) -> Dict[str, str]:
    blank_label = layout["response_labels"]["blank"]
    double_label = layout["response_labels"]["double"]

    answers: Dict[str, str] = {}
    for item in layout["answers"]:
        question = int(item["question"])
        ratios = {
            label: circle_fill_ratio(binary_inv, spec["x_px"], spec["y_px"], spec["radius_px"])
            for label, spec in item["choices"].items()
        }
        answers[f"P{question}"] = decode_single_mark(ratios, threshold, blank_label, double_label)
    return answers


def compute_chilean_dv(number: str) -> str:
    reversed_digits = list(map(int, reversed(number)))
    factors = [2, 3, 4, 5, 6, 7]
    total = sum(d * factors[i % len(factors)] for i, d in enumerate(reversed_digits))
    remainder = 11 - (total % 11)
    if remainder == 11:
        return "0"
    if remainder == 10:
        return "K"
    return str(remainder)


def decode_rut(binary_inv: np.ndarray, layout: dict, threshold: float) -> Tuple[str, str]:
    columns = layout["rut"]["columns"]
    raw_chars: List[str] = []
    incomplete = False
    multiple = False

    for column in columns:
        ratios = {
            label: circle_fill_ratio(binary_inv, spec["x_px"], spec["y_px"], spec["radius_px"])
            for label, spec in column["options"].items()
        }
        decoded = decode_single_mark(ratios, threshold, blank_label="?", multiple_label="*")
        if decoded == "?":
            incomplete = True
        elif decoded == "*":
            multiple = True
        raw_chars.append(decoded)

    number_part = "".join(raw_chars[:8])
    dv_part = raw_chars[8] if len(raw_chars) > 8 else "?"
    rut_text = f"{number_part}-{dv_part}"

    states: List[str] = []
    if incomplete:
        states.append(layout["rut"]["labels"]["incomplete"])
    if multiple:
        states.append(layout["rut"]["labels"]["multiple"])

    if not incomplete and not multiple and number_part.isdigit() and dv_part in "0123456789K":
        expected = compute_chilean_dv(number_part)
        if dv_part != expected:
            states.append(layout["rut"]["labels"]["dv_mismatch"])

    if not states:
        states.append(layout["rut"]["labels"]["ok"])

    return rut_text, "+".join(states)


def annotate_debug_image(
    warped: np.ndarray,
    layout: dict,
    answers: Dict[str, str],
    rut_text: str,
    rut_state: str,
    output_path: Path,
) -> None:
    debug = warped.copy()

    for item in layout["answers"]:
        q = int(item["question"])
        selected = answers[f"P{q}"]
        for label, spec in item["choices"].items():
            center = (int(round(spec["x_px"])), int(round(spec["y_px"])))
            radius = int(round(spec["radius_px"]))
            color = (0, 160, 0) if selected == label else (100, 100, 100)
            if selected in {"RD", "N"}:
                color = (0, 0, 255) if selected == "RD" else (0, 165, 255)
            cv2.circle(debug, center, radius, color, 2)

    cv2.putText(
        debug,
        f"RUT: {rut_text} [{rut_state}]",
        (40, 70),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.8,
        (40, 40, 200) if rut_state != "OK" else (0, 120, 0),
        2,
        cv2.LINE_AA,
    )
    cv2.imwrite(str(output_path), debug)


def process_image(
    image_path: Path,
    layout: dict,
    answer_threshold: float,
    rut_threshold: float,
    debug_dir: Optional[Path] = None,
) -> Dict[str, str]:
    image = read_image(image_path)
    gray = to_gray(image)
    detected_markers = detect_markers(gray, layout)
    warped = warp_to_template(image, detected_markers, layout)
    warped_gray = to_gray(warped)
    binary_inv = binarize(warped_gray)

    answers = decode_answers(binary_inv, layout, answer_threshold)
    rut_text, rut_state = decode_rut(binary_inv, layout, rut_threshold)

    if debug_dir is not None:
        debug_dir.mkdir(parents=True, exist_ok=True)
        annotate_debug_image(
            warped,
            layout,
            answers,
            rut_text,
            rut_state,
            debug_dir / f"{image_path.stem}_debug.png",
        )

    row = {
        "ARCHIVO": image_path.name,
        "RUT": rut_text,
        "RUT_ESTADO": rut_state,
    }
    row.update(answers)
    return row


def make_failure_row(image_path: Path, n_questions: int, error_code: str, message: str) -> Dict[str, str]:
    row = {
        "ARCHIVO": image_path.name,
        "RUT": "",
        "RUT_ESTADO": error_code,
        "OBSERVACION": message,
    }
    for q in range(1, n_questions + 1):
        row[f"P{q}"] = ""
    return row


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lee una carpeta de hojas escaneadas y genera un Excel con RUT y respuestas."
    )
    parser.add_argument("--input-dir", required=True, type=Path, help="Carpeta con imagenes JPG/PNG/TIFF.")
    parser.add_argument("--output", required=True, type=Path, help="Ruta del Excel de salida.")
    parser.add_argument(
        "--layout",
        type=Path,
        default=Path(__file__).resolve().with_name("hoja_respuestas_v1_layout.json"),
        help="Archivo JSON con la geometria de la plantilla.",
    )
    parser.add_argument("--answer-threshold", type=float, default=None, help="Umbral de marcado para respuestas.")
    parser.add_argument("--rut-threshold", type=float, default=None, help="Umbral de marcado para el RUT.")
    parser.add_argument("--debug-dir", type=Path, default=None, help="Carpeta opcional para guardar imagenes de depuracion.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    layout = load_layout(args.layout)

    answer_threshold = (
        args.answer_threshold
        if args.answer_threshold is not None
        else float(layout["reader_defaults"]["answer_mark_threshold"])
    )
    rut_threshold = (
        args.rut_threshold
        if args.rut_threshold is not None
        else float(layout["reader_defaults"]["rut_mark_threshold"])
    )

    files = list_images(args.input_dir)
    rows: List[Dict[str, str]] = []

    total_questions = len(layout["answers"])
    for image_path in files:
        try:
            row = process_image(
                image_path=image_path,
                layout=layout,
                answer_threshold=answer_threshold,
                rut_threshold=rut_threshold,
                debug_dir=args.debug_dir,
            )
        except MarkerDetectionError as exc:
            row = make_failure_row(image_path, total_questions, "IMG", str(exc))
        except Exception as exc:
            row = make_failure_row(image_path, total_questions, "ERR", str(exc))
        rows.append(row)

    df = pd.DataFrame(rows)

    ordered_columns = ["ARCHIVO", "RUT", "RUT_ESTADO"]
    if "OBSERVACION" in df.columns:
        ordered_columns.append("OBSERVACION")
    ordered_columns.extend([f"P{q}" for q in range(1, total_questions + 1)])
    df = df.reindex(columns=ordered_columns)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(args.output, index=False)

    print(f"Excel generado: {args.output}")
    print("Codigos usados: N = sin responder, RD = respuesta doble, RI = RUT incompleto, RM = RUT multiple, DV = digito verificador inconsistente, IMG = no se detectaron marcadores.")


if __name__ == "__main__":
    main()
