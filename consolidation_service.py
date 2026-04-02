
from __future__ import annotations
from pathlib import Path
import subprocess, sys

def run_consolidation(scan_results_path: Path, inscritos_path: Path, instrumento_path: Path, output_path: Path, report_dir: Path | None = None) -> None:
    script = Path(__file__).resolve().with_name('consolidar_resultados_con_instrumento.py')
    cmd = [sys.executable, str(script), '--scan-results', str(scan_results_path), '--inscritos', str(inscritos_path), '--instrumento', str(instrumento_path), '--output', str(output_path)]
    if report_dir is not None:
        cmd += ['--report-dir', str(report_dir)]
    subprocess.run(cmd, check=True)
