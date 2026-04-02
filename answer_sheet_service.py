from __future__ import annotations
from pathlib import Path
import shutil
import subprocess
import sys

def generate_answer_sheet(output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    script = Path(__file__).resolve().with_name("generate_answer_sheet_v1.py")
    script_dir = script.parent

    pdf_src = script_dir / "hoja_respuestas_v1_carta.pdf"
    json_src = script_dir / "hoja_respuestas_v1_layout.json"

    subprocess.run([sys.executable, str(script)], cwd=str(script_dir), check=True)

    if not pdf_src.exists():
        raise FileNotFoundError(f"No se generó el PDF esperado: {pdf_src}")
    if not json_src.exists():
        raise FileNotFoundError(f"No se generó el layout esperado: {json_src}")

    pdf_dst = output_dir / pdf_src.name
    json_dst = output_dir / json_src.name
    shutil.copy2(pdf_src, pdf_dst)
    shutil.copy2(json_src, json_dst)
    return pdf_dst, json_dst
