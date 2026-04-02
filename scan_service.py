
from __future__ import annotations
from pathlib import Path
import subprocess, sys
from typing import Optional

def run_scan(input_dir: Path, output_path: Path, layout_path: Path, answer_threshold: Optional[float]=None, rut_threshold: Optional[float]=None, debug_dir: Optional[Path]=None) -> None:
    script = Path(__file__).resolve().with_name('scan_respuestas_v1.py')
    cmd = [sys.executable, str(script), '--input-dir', str(input_dir), '--output', str(output_path), '--layout', str(layout_path)]
    if answer_threshold is not None:
        cmd += ['--answer-threshold', str(answer_threshold)]
    if rut_threshold is not None:
        cmd += ['--rut-threshold', str(rut_threshold)]
    if debug_dir is not None:
        cmd += ['--debug-dir', str(debug_dir)]
    subprocess.run(cmd, check=True)
