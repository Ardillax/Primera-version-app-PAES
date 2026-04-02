from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional
import shutil, zipfile
from answer_sheet_service import generate_answer_sheet
from scan_service import run_scan
from consolidation_service import run_consolidation

ALLOWED_SCAN_EXTENSIONS = {'.jpg','.jpeg','.png','.tif','.tiff','.bmp','.webp'}

@dataclass
class PipelineResult:
    resultados_xlsx: Optional[Path]=None
    consolidados_xlsx: Optional[Path]=None
    report_dir: Optional[Path]=None


def prepare_workspace(base_dir: Path) -> dict[str, Path]:
    paths = {
        'base': base_dir,
        'inputs': base_dir / 'inputs',
        'scans': base_dir / 'inputs' / 'scans',
        'outputs': base_dir / 'outputs',
        'debug': base_dir / 'outputs' / 'debug',
        'reports': base_dir / 'outputs' / 'reportes_html',
        'generated': base_dir / 'generated',
        'templates': base_dir / 'templates',
    }
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    return paths


def save_canonical_upload(uploaded_file, destination_dir: Path, canonical_name: str) -> Path:
    destination_dir.mkdir(parents=True, exist_ok=True)
    suffix = Path(uploaded_file.name).suffix.lower() or Path(canonical_name).suffix.lower()
    target = destination_dir / (Path(canonical_name).stem + suffix)
    target.write_bytes(uploaded_file.getbuffer())
    return target


def extract_scan_zip(zip_path: Path, destination_dir: Path) -> list[Path]:
    destination_dir.mkdir(parents=True, exist_ok=True)
    extracted=[]
    with zipfile.ZipFile(zip_path,'r') as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            name = Path(member.filename).name
            if not name or Path(name).suffix.lower() not in ALLOWED_SCAN_EXTENSIONS:
                continue
            target = destination_dir / name
            with zf.open(member) as src, open(target,'wb') as dst:
                shutil.copyfileobj(src,dst)
            extracted.append(target)
    return extracted


def copy_scan_files(files: Iterable, destination_dir: Path) -> list[Path]:
    destination_dir.mkdir(parents=True, exist_ok=True)
    saved=[]
    for uploaded_file in files:
        ext = Path(uploaded_file.name).suffix.lower()
        if ext not in ALLOWED_SCAN_EXTENSIONS:
            continue
        target = destination_dir / Path(uploaded_file.name).name
        target.write_bytes(uploaded_file.getbuffer())
        saved.append(target)
    return saved


def run_full_pipeline(workspace_dir: Path, inscritos_path: Path, instrumento_path: Path, layout_path: Path, answer_threshold: Optional[float]=None, rut_threshold: Optional[float]=None, debug: bool=False) -> PipelineResult:
    paths = prepare_workspace(workspace_dir)
    resultados_path = paths['outputs'] / 'resultados.xlsx'
    consolidados_path = paths['outputs'] / 'resultados_consolidados.xlsx'
    debug_dir = paths['debug'] if debug else None
    run_scan(paths['scans'], resultados_path, layout_path, answer_threshold, rut_threshold, debug_dir)
    run_consolidation(resultados_path, inscritos_path, instrumento_path, consolidados_path, paths['reports'])
    return PipelineResult(resultados_xlsx=resultados_path, consolidados_xlsx=consolidados_path, report_dir=paths['reports'])
