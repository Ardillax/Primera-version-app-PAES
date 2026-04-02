from __future__ import annotations
from datetime import datetime
from pathlib import Path
import io
import shutil
import tempfile
import zipfile

import pandas as pd
import streamlit as st

from pipeline import (
    copy_scan_files,
    extract_scan_zip,
    generate_answer_sheet,
    prepare_workspace,
    run_full_pipeline,
    save_canonical_upload,
)
from template_service import build_input_templates

st.set_page_config(page_title='Scan Respuestas PAES', page_icon='📝', layout='wide')

FORMAT_KEYS = {
    'pdf': 'format_pdf_bytes',
    'pdf_name': 'format_pdf_name',
    'inscritos': 'format_inscritos_bytes',
    'inscritos_name': 'format_inscritos_name',
    'instrumento': 'format_instrumento_bytes',
    'instrumento_name': 'format_instrumento_name',
}
RESULT_KEYS = {
    'resultados': 'resultados_bytes',
    'resultados_name': 'resultados_name',
    'consolidado': 'consolidado_bytes',
    'consolidado_name': 'consolidado_name',
    'reportes': 'reportes_zip_bytes',
    'reportes_name': 'reportes_zip_name',
    'debug': 'debug_zip_bytes',
    'debug_name': 'debug_zip_name',
}


def make_session_workspace() -> Path:
    if 'workspace_dir' not in st.session_state:
        st.session_state.workspace_dir = str(Path(tempfile.mkdtemp(prefix='scan_respuestas_')))
    return Path(st.session_state.workspace_dir)


def zip_directory_bytes(source_dir: Path) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file in source_dir.rglob('*'):
            if file.is_file():
                zf.write(file, arcname=file.relative_to(source_dir))
    return buffer.getvalue()


def detect_duplicate_ruts(inscritos_path: Path) -> pd.DataFrame:
    try:
        df = pd.read_excel(inscritos_path)
    except Exception:
        df = pd.read_csv(inscritos_path)
    candidate = None
    for col in df.columns:
        norm = str(col).strip().lower()
        if 'rut' in norm:
            candidate = col
            break
    if candidate is None:
        return pd.DataFrame()
    ruts = df[candidate].astype(str).str.upper().str.replace(r'[^0-9K]', '', regex=True)
    ruts = ruts.where(ruts.str.len() > 1, '')
    normalized = ruts.where(ruts == '', ruts.str[:-1] + '-' + ruts.str[-1])
    out = normalized.value_counts().reset_index()
    out.columns = ['rut_normalizado', 'veces']
    return out[out['veces'] > 1]


def set_format_downloads(pdf_path: Path, template_inscritos: Path, template_instrumento: Path) -> None:
    st.session_state[FORMAT_KEYS['pdf']] = pdf_path.read_bytes()
    st.session_state[FORMAT_KEYS['pdf_name']] = pdf_path.name
    st.session_state[FORMAT_KEYS['inscritos']] = template_inscritos.read_bytes()
    st.session_state[FORMAT_KEYS['inscritos_name']] = template_inscritos.name
    st.session_state[FORMAT_KEYS['instrumento']] = template_instrumento.read_bytes()
    st.session_state[FORMAT_KEYS['instrumento_name']] = template_instrumento.name


def show_format_downloads() -> None:
    if FORMAT_KEYS['pdf'] in st.session_state:
        st.download_button(
            'Descargar hoja de respuestas (PDF)',
            st.session_state[FORMAT_KEYS['pdf']],
            file_name=st.session_state[FORMAT_KEYS['pdf_name']],
            mime='application/pdf',
        )
    if FORMAT_KEYS['inscritos'] in st.session_state:
        st.download_button(
            'Descargar plantilla Inscritos (XLSX)',
            st.session_state[FORMAT_KEYS['inscritos']],
            file_name=st.session_state[FORMAT_KEYS['inscritos_name']],
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )
    if FORMAT_KEYS['instrumento'] in st.session_state:
        st.download_button(
            'Descargar plantilla Instrumento (XLSX)',
            st.session_state[FORMAT_KEYS['instrumento']],
            file_name=st.session_state[FORMAT_KEYS['instrumento_name']],
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )


def set_result_downloads(resultados_path: Path | None, consolidados_path: Path | None, report_dir: Path | None, debug_dir: Path | None) -> None:
    if resultados_path and resultados_path.exists():
        st.session_state[RESULT_KEYS['resultados']] = resultados_path.read_bytes()
        st.session_state[RESULT_KEYS['resultados_name']] = resultados_path.name
    if consolidados_path and consolidados_path.exists():
        st.session_state[RESULT_KEYS['consolidado']] = consolidados_path.read_bytes()
        st.session_state[RESULT_KEYS['consolidado_name']] = consolidados_path.name
    if report_dir and report_dir.exists() and any(report_dir.iterdir()):
        st.session_state[RESULT_KEYS['reportes']] = zip_directory_bytes(report_dir)
        st.session_state[RESULT_KEYS['reportes_name']] = f'reportes_html_{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'
    if debug_dir and debug_dir.exists() and any(debug_dir.iterdir()):
        st.session_state[RESULT_KEYS['debug']] = zip_directory_bytes(debug_dir)
        st.session_state[RESULT_KEYS['debug_name']] = f'debug_{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'


def show_result_downloads() -> None:
    if RESULT_KEYS['resultados'] in st.session_state:
        st.download_button(
            'Descargar resultados.xlsx',
            st.session_state[RESULT_KEYS['resultados']],
            file_name=st.session_state[RESULT_KEYS['resultados_name']],
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )
    if RESULT_KEYS['consolidado'] in st.session_state:
        st.download_button(
            'Descargar resultados_consolidados.xlsx',
            st.session_state[RESULT_KEYS['consolidado']],
            file_name=st.session_state[RESULT_KEYS['consolidado_name']],
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )
    if RESULT_KEYS['reportes'] in st.session_state:
        st.download_button(
            'Descargar reportes_html.zip',
            st.session_state[RESULT_KEYS['reportes']],
            file_name=st.session_state[RESULT_KEYS['reportes_name']],
            mime='application/zip',
        )
    if RESULT_KEYS['debug'] in st.session_state:
        st.download_button(
            'Descargar debug.zip',
            st.session_state[RESULT_KEYS['debug']],
            file_name=st.session_state[RESULT_KEYS['debug_name']],
            mime='application/zip',
        )


def main() -> None:
    st.title('Scan Respuestas PAES')
    st.write('Genera formatos, procesa scans OMR y consolida resultados en un solo flujo.')
    workspace_dir = make_session_workspace()
    paths = prepare_workspace(workspace_dir)

    with st.sidebar:
        st.subheader('Sesión')
        st.caption('En despliegue web, la app no dependerá de tu PC encendido.')
        if st.button('Limpiar sesión'):
            shutil.rmtree(workspace_dir, ignore_errors=True)
            st.session_state.clear()
            st.rerun()

    tab1, tab2 = st.tabs(['1. Formatos', '2. Procesar scans y consolidar'])

    with tab1:
        st.subheader('Formatos')
        st.caption('Descarga la hoja de respuestas y las plantillas de los archivos de entrada.')
        if st.button('Preparar formatos', type='primary'):
            with st.spinner('Generando formatos...'):
                pdf_path, _ = generate_answer_sheet(paths['generated'])
                template_inscritos, template_instrumento = build_input_templates(paths['templates'])
                set_format_downloads(pdf_path, template_inscritos, template_instrumento)
            st.success('Formatos generados correctamente.')
        show_format_downloads()

    with tab2:
        st.subheader('Procesar scans y consolidar')
        st.caption('Puedes subir Inscritos, Instrumento y los scans con cualquier nombre. Los scans aceptan un ZIP o imágenes sueltas.')
        col1, col2 = st.columns(2)
        with col1:
            inscritos_file = st.file_uploader('Inscritos', type=['xlsx', 'csv'], key='inscritos')
            instrumento_file = st.file_uploader('Instrumento', type=['xlsx', 'csv'], key='instrumento')
        with col2:
            scans_uploads = st.file_uploader(
                'Scans (ZIP o imágenes)',
                type=['zip', 'jpg', 'jpeg', 'png', 'tif', 'tiff', 'bmp', 'webp'],
                accept_multiple_files=True,
                key='scans_uploads',
            )

        with st.expander('Modo avanzado'):
            debug = st.checkbox('Guardar imágenes debug', value=False, help='Útil para revisar alineación, respuestas detectadas y decodificación del RUT.')
            answer_threshold = st.number_input('Umbral respuestas', value=0.28, step=0.01, format='%.2f', help='Más bajo = más sensible; más alto = más estricto.')
            rut_threshold = st.number_input('Umbral RUT', value=0.26, step=0.01, format='%.2f', help='Ajusta la detección de marcas del RUT.')

        can_run = inscritos_file is not None and instrumento_file is not None and scans_uploads and len(scans_uploads) > 0
        if st.button('Procesar y consolidar', type='primary', disabled=not can_run):
            with st.spinner('Preparando archivos...'):
                inputs_dir = paths['inputs']
                scans_dir = paths['scans']
                shutil.rmtree(scans_dir, ignore_errors=True)
                scans_dir.mkdir(parents=True, exist_ok=True)

                inscritos_path = save_canonical_upload(inscritos_file, inputs_dir, 'inscritos.xlsx')
                instrumento_path = save_canonical_upload(instrumento_file, inputs_dir, 'instrumento.xlsx')
                dup_df = detect_duplicate_ruts(inscritos_path)

                generated_layout = paths['generated'] / 'hoja_respuestas_v1_layout.json'
                if generated_layout.exists():
                    layout_path = generated_layout
                else:
                    _, layout_path = generate_answer_sheet(paths['generated'])

                scan_count = 0
                for uploaded in scans_uploads:
                    ext = Path(uploaded.name).suffix.lower()
                    if ext == '.zip':
                        zip_path = save_canonical_upload(uploaded, inputs_dir, f'scans_{scan_count}.zip')
                        scan_count += len(extract_scan_zip(zip_path, scans_dir))
                    else:
                        scan_count += len(copy_scan_files([uploaded], scans_dir))

            if scan_count == 0:
                st.error('No se encontraron imágenes válidas en los scans subidos.')
            else:
                if not dup_df.empty:
                    st.warning(
                        f'Se detectaron {len(dup_df)} RUT duplicados en Inscritos. Se considerarán usando la primera fila por RUT, '
                        'aparecerán advertidos en el consolidado y podrán seguir al mail merge.'
                    )
                    st.dataframe(dup_df, use_container_width=True)
                with st.spinner('Leyendo hojas y consolidando...'):
                    result = run_full_pipeline(
                        workspace_dir=workspace_dir,
                        inscritos_path=inscritos_path,
                        instrumento_path=instrumento_path,
                        layout_path=layout_path,
                        answer_threshold=answer_threshold,
                        rut_threshold=rut_threshold,
                        debug=debug,
                    )
                    set_result_downloads(result.resultados_xlsx, result.consolidados_xlsx, result.report_dir, paths['debug'] if debug else None)
                st.success('Proceso completado.')

        show_result_downloads()

    st.info('Cuando la app se despliegue en Streamlit Community Cloud, podrás compartir una URL y ya no dependerá de tu PC encendido.')


if __name__ == '__main__':
    main()
