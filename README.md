
# Scan Respuestas PAES - App Streamlit

## Ejecutar local
1. Crear entorno virtual
2. `python -m pip install -r requirements.txt`
3. `python -m streamlit run streamlit_app.py`

## Qué hace
- Descarga formatos: hoja de respuestas, layout, plantilla de inscritos, plantilla de instrumento.
- Procesa scans con cualquier nombre de archivo.
- Renombra internamente a nombres canónicos temporales.
- Consolida resultados.
- Advierte duplicados de inscritos sin bloquear `mail_merge`.

## Despliegue
Cuando la subas a Streamlit Community Cloud, la app quedará disponible en una URL web y ya no dependerá de tu PC encendido.
