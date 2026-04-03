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


st.set_page_config(
    page_title="Plataforma PAES M2",
    page_icon="📘",
    layout="wide",
)

st.markdown(
    """
    <style>
        :root {
            --udec-blue: #003DA5;
            --udec-blue-dark: #002B73;
            --udec-blue-soft: #EAF1FF;
            --udec-gold: #F2C94C;
            --text-main: #0F172A;
            --text-soft: #475569;
            --border-soft: #E5E7EB;
            --bg-soft: #F8FAFC;
        }

        .block-container {
            padding-top: 1.2rem;
            padding-bottom: 2rem;
            max-width: 1200px;
        }

        .hero {
            background: linear-gradient(135deg, var(--udec-blue) 0%, var(--udec-blue-dark) 100%);
            border-radius: 22px;
            padding: 1.6rem 1.8rem;
            margin-bottom: 1.2rem;
            color: white;
            box-shadow: 0 10px 24px rgba(0, 61, 165, 0.12);
        }

        .hero-title {
            font-size: 2.25rem;
            font-weight: 800;
            line-height: 1.15;
            margin: 0 0 0.35rem 0;
        }

        .hero-subtitle {
            font-size: 1rem;
            opacity: 0.95;
            margin: 0;
        }

        .mini-pill {
            display: inline-block;
            background: rgba(255, 255, 255, 0.12);
            border: 1px solid rgba(255, 255, 255, 0.14);
            color: white;
            padding: 0.35rem 0.75rem;
            border-radius: 999px;
            font-size: 0.88rem;
            font-weight: 600;
            margin-bottom: 0.9rem;
        }

        .section-title {
            font-size: 1.25rem;
            font-weight: 700;
            color: var(--text-main);
            margin-bottom: 0.25rem;
        }

        .section-subtitle {
            font-size: 0.95rem;
            color: var(--text-soft);
            margin-bottom: 0.75rem;
        }

        .note-box {
            background: var(--bg-soft);
            border: 1px dashed #CBD5E1;
            border-radius: 14px;
            padding: 0.8rem 1rem;
            color: var(--text-soft);
            font-size: 0.92rem;
            margin-top: 0.75rem;
            margin-bottom: 0.25rem;
        }

        .small-muted {
            color: #64748B;
            font-size: 0.92rem;
        }

        .stButton > button,
        .stDownloadButton > button {
            border-radius: 12px;
            font-weight: 700;
            min-height: 2.7rem;
            border: 1px solid #D7E0F1;
        }

        .stTabs [data-baseweb="tab-list"] {
            gap: 0.6rem;
        }

        .stTabs [data-baseweb="tab"] {
            border-radius: 12px 12px 0 0;
            padding-left: 1rem;
            padding-right: 1rem;
            font-weight: 700;
        }

        div[data-testid="stFileUploader"] section {
            border-radius: 14px;
        }

        div[data-testid="stAlert"] {
            border-radius: 14px;
        }

        [data-testid="stSidebar"] {
            background: #FBFDFF;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

FORMAT_KEYS = {
    "pdf": "format_pdf_bytes",
    "pdf_name": "format_pdf_name",
    "inscritos": "format_inscritos_bytes",
    "inscritos_name": "format_inscritos_name",
    "instrumento": "format_instrumento_bytes",
    "instrumento_name": "format_instrumento_name",
}

RESULT_KEYS = {
    "resultados": "resultados_bytes",
    "resultados_name": "resultados_name",
    "consolidado": "consolidado_bytes",
    "consolidado_name": "consolidado_name",
    "reportes": "reportes_zip_bytes",
    "reportes_name": "reportes_zip_name",
    "debug": "debug_zip_bytes",
    "debug_name": "debug_zip_name",
}


def make_session_workspace() -> Path:
    if "workspace_dir" not in st.session_state:
        st.session_state.workspace_dir = str(
            Path(tempfile.mkdtemp(prefix="scan_respuestas_"))
        )
    return Path(st.session_state.workspace_dir)


def reset_session_workspace() -> None:
    if "workspace_dir" in st.session_state:
        shutil.rmtree(st.session_state["workspace_dir"], ignore_errors=True)
    keys_to_clear = list(FORMAT_KEYS.values()) + list(RESULT_KEYS.values()) + ["workspace_dir"]
    for key in keys_to_clear:
        st.session_state.pop(key, None)


def zip_directory_bytes(source_dir: Path) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for file in source_dir.rglob("*"):
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
        if "rut" in norm:
            candidate = col
            break

    if candidate is None:
        return pd.DataFrame()

    ruts = (
        df[candidate]
        .astype(str)
        .str.upper()
        .str.replace(r"[^0-9K]", "", regex=True)
    )
    ruts = ruts.where(ruts.str.len() > 1, "")
    normalized = ruts.where(ruts == "", ruts.str[:-1] + "-" + ruts.str[-1])

    out = normalized.value_counts().reset_index()
    out.columns = ["rut_normalizado", "veces"]
    return out[out["veces"] > 1]


def set_format_downloads(
    pdf_path: Path,
    template_inscritos: Path,
    template_instrumento: Path,
) -> None:
    if not pdf_path.exists():
        raise FileNotFoundError(f"No se encontró el PDF generado: {pdf_path}")
    if not template_inscritos.exists():
        raise FileNotFoundError(
            f"No se encontró la plantilla de inscritos: {template_inscritos}"
        )
    if not template_instrumento.exists():
        raise FileNotFoundError(
            f"No se encontró la plantilla de instrumento: {template_instrumento}"
        )

    st.session_state[FORMAT_KEYS["pdf"]] = pdf_path.read_bytes()
    st.session_state[FORMAT_KEYS["pdf_name"]] = pdf_path.name
    st.session_state[FORMAT_KEYS["inscritos"]] = template_inscritos.read_bytes()
    st.session_state[FORMAT_KEYS["inscritos_name"]] = template_inscritos.name
    st.session_state[FORMAT_KEYS["instrumento"]] = template_instrumento.read_bytes()
    st.session_state[FORMAT_KEYS["instrumento_name"]] = template_instrumento.name


def show_format_downloads() -> None:
    c1, c2, c3 = st.columns(3)

    with c1:
        if FORMAT_KEYS["pdf"] in st.session_state:
            st.download_button(
                "Descargar hoja de respuestas",
                st.session_state[FORMAT_KEYS["pdf"]],
                file_name=st.session_state[FORMAT_KEYS["pdf_name"]],
                mime="application/pdf",
                use_container_width=True,
            )

    with c2:
        if FORMAT_KEYS["inscritos"] in st.session_state:
            st.download_button(
                "Descargar plantilla Inscritos",
                st.session_state[FORMAT_KEYS["inscritos"]],
                file_name=st.session_state[FORMAT_KEYS["inscritos_name"]],
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

    with c3:
        if FORMAT_KEYS["instrumento"] in st.session_state:
            st.download_button(
                "Descargar plantilla Instrumento",
                st.session_state[FORMAT_KEYS["instrumento"]],
                file_name=st.session_state[FORMAT_KEYS["instrumento_name"]],
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )


def set_result_downloads(
    resultados_path: Path | None,
    consolidados_path: Path | None,
    report_dir: Path | None,
    debug_dir: Path | None,
) -> None:
    if resultados_path and resultados_path.exists():
        st.session_state[RESULT_KEYS["resultados"]] = resultados_path.read_bytes()
        st.session_state[RESULT_KEYS["resultados_name"]] = resultados_path.name

    if consolidados_path and consolidados_path.exists():
        st.session_state[RESULT_KEYS["consolidado"]] = consolidados_path.read_bytes()
        st.session_state[RESULT_KEYS["consolidado_name"]] = consolidados_path.name

    if report_dir and report_dir.exists() and any(report_dir.iterdir()):
        st.session_state[RESULT_KEYS["reportes"]] = zip_directory_bytes(report_dir)
        st.session_state[RESULT_KEYS["reportes_name"]] = (
            f"reportes_html_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        )

    if debug_dir and debug_dir.exists() and any(debug_dir.iterdir()):
        st.session_state[RESULT_KEYS["debug"]] = zip_directory_bytes(debug_dir)
        st.session_state[RESULT_KEYS["debug_name"]] = (
            f"debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        )


def show_result_downloads() -> None:
    c1, c2 = st.columns(2)

    with c1:
        if RESULT_KEYS["resultados"] in st.session_state:
            st.download_button(
                "Descargar resultados.xlsx",
                st.session_state[RESULT_KEYS["resultados"]],
                file_name=st.session_state[RESULT_KEYS["resultados_name"]],
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

        if RESULT_KEYS["reportes"] in st.session_state:
            st.download_button(
                "Descargar reportes_html.zip",
                st.session_state[RESULT_KEYS["reportes"]],
                file_name=st.session_state[RESULT_KEYS["reportes_name"]],
                mime="application/zip",
                use_container_width=True,
            )

    with c2:
        if RESULT_KEYS["consolidado"] in st.session_state:
            st.download_button(
                "Descargar resultados_consolidados.xlsx",
                st.session_state[RESULT_KEYS["consolidado"]],
                file_name=st.session_state[RESULT_KEYS["consolidado_name"]],
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

        if RESULT_KEYS["debug"] in st.session_state:
            st.download_button(
                "Descargar debug.zip",
                st.session_state[RESULT_KEYS["debug"]],
                file_name=st.session_state[RESULT_KEYS["debug_name"]],
                mime="application/zip",
                use_container_width=True,
            )


def render_header() -> None:
    st.markdown(
        """
        <div class="hero">
            <div class="mini-pill">Facultad de Ingeniería · Universidad de Concepción</div>
            <div class="hero-title">Plataforma de corrección PAES M2</div>
            <p class="hero-subtitle">
                Genera formatos, procesa hojas escaneadas y descarga resultados consolidados
                desde una sola interfaz.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_heading(title: str, subtitle: str | None = None) -> None:
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)
    if subtitle:
        st.markdown(
            f'<div class="section-subtitle">{subtitle}</div>',
            unsafe_allow_html=True,
        )


def main() -> None:
    workspace_dir = make_session_workspace()
    paths = prepare_workspace(workspace_dir)

    render_header()

    with st.sidebar:
        st.subheader("Sesión")
        st.markdown(
            '<div class="small-muted">Limpia la sesión si quieres reiniciar el trabajo actual.</div>',
            unsafe_allow_html=True,
        )
        st.markdown("---")
        if st.button("Limpiar sesión", use_container_width=True):
            reset_session_workspace()
            st.rerun()

    tab_formatos, tab_procesar = st.tabs(["Formatos", "Procesar"])

    with tab_formatos:
        with st.container(border=True):
            section_heading(
                "Formatos base",
                "Descarga la hoja de respuestas y las plantillas necesarias para comenzar.",
            )

            col_left, col_right = st.columns([1.3, 1])
            with col_left:
                if st.button("Preparar formatos", type="primary", use_container_width=False):
                    try:
                        with st.spinner("Generando formatos..."):
                            pdf_path, _layout_path = generate_answer_sheet(paths["generated"])
                            template_inscritos, template_instrumento = build_input_templates(
                                paths["templates"]
                            )
                            set_format_downloads(pdf_path, template_inscritos, template_instrumento)
                        st.success("Formatos preparados correctamente.")
                    except Exception as e:
                        st.error(f"No se pudieron generar los formatos: {e}")

            with col_right:
                st.markdown(
                    """
                    <div class="note-box">
                        Descarga estos archivos una sola vez y reutilízalos cuando lo necesites.
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            st.divider()
            show_format_downloads()

    with tab_procesar:
        with st.container(border=True):
            section_heading(
                "Archivos de entrada",
                "Sube inscritos, instrumento y los scans. Los scans aceptan ZIP e imágenes sueltas.",
            )

            col1, col2 = st.columns(2)
            with col1:
                inscritos_file = st.file_uploader(
                    "Inscritos",
                    type=["xlsx", "csv"],
                    key="inscritos",
                )
                instrumento_file = st.file_uploader(
                    "Instrumento",
                    type=["xlsx", "csv"],
                    key="instrumento",
                )

            with col2:
                scans_uploads = st.file_uploader(
                    "Scans",
                    type=["zip", "jpg", "jpeg", "png", "tif", "tiff", "bmp"],
                    accept_multiple_files=True,
                    key="scans_uploads",
                    help="Puedes subir un ZIP, imágenes sueltas o una mezcla de ambos.",
                )

            with st.expander("Modo avanzado"):
                debug = st.checkbox(
                    "Guardar imágenes debug",
                    value=False,
                    help="Útil para revisar alineación, respuestas detectadas y lectura del RUT.",
                )
                answer_threshold = st.number_input(
                    "Umbral respuestas",
                    value=0.28,
                    step=0.01,
                    format="%.2f",
                )
                rut_threshold = st.number_input(
                    "Umbral RUT",
                    value=0.26,
                    step=0.01,
                    format="%.2f",
                )

            can_run = (
                inscritos_file is not None
                and instrumento_file is not None
                and scans_uploads is not None
                and len(scans_uploads) > 0
            )

            st.divider()

            if st.button("Procesar y consolidar", type="primary", disabled=not can_run):
                try:
                    with st.spinner("Preparando archivos..."):
                        inputs_dir = paths["inputs"]
                        scans_dir = paths["scans"]

                        shutil.rmtree(scans_dir, ignore_errors=True)
                        scans_dir.mkdir(parents=True, exist_ok=True)

                        inscritos_path = save_canonical_upload(
                            inscritos_file, inputs_dir, "inscritos.xlsx"
                        )
                        instrumento_path = save_canonical_upload(
                            instrumento_file, inputs_dir, "instrumento.xlsx"
                        )

                        dup_df = detect_duplicate_ruts(inscritos_path)

                        generated_layout = paths["generated"] / "hoja_respuestas_v1_layout.json"
                        if generated_layout.exists():
                            layout_path = generated_layout
                        else:
                            _, layout_path = generate_answer_sheet(paths["generated"])

                        scan_count = 0
                        for uploaded in scans_uploads:
                            ext = Path(uploaded.name).suffix.lower()
                            if ext == ".zip":
                                zip_path = save_canonical_upload(
                                    uploaded, inputs_dir, f"scans_{scan_count}.zip"
                                )
                                scan_count += len(extract_scan_zip(zip_path, scans_dir))
                            else:
                                scan_count += len(copy_scan_files([uploaded], scans_dir))

                    if scan_count == 0:
                        st.error("No se encontraron imágenes válidas en los scans subidos.")
                    else:
                        if not dup_df.empty:
                            st.warning(
                                f"Se detectaron {len(dup_df)} RUT duplicados en Inscritos. "
                                "Se considerarán usando la primera fila por RUT y quedarán advertidos en el consolidado."
                            )
                            st.dataframe(dup_df, use_container_width=True)

                        with st.spinner("Leyendo hojas y consolidando..."):
                            result = run_full_pipeline(
                                workspace_dir=workspace_dir,
                                inscritos_path=inscritos_path,
                                instrumento_path=instrumento_path,
                                layout_path=layout_path,
                                answer_threshold=answer_threshold,
                                rut_threshold=rut_threshold,
                                debug=debug,
                            )
                            set_result_downloads(
                                result.resultados_xlsx,
                                result.consolidados_xlsx,
                                result.report_dir,
                                paths["debug"] if debug else None,
                            )

                        st.success("Proceso completado.")
                except Exception as e:
                    st.error(f"Ocurrió un problema durante el procesamiento: {e}")

        with st.container(border=True):
            section_heading(
                "Descargas",
                "Los archivos generados permanecen disponibles hasta que vuelvas a procesar.",
            )
            show_result_downloads()


if __name__ == "__main__":
    main()
