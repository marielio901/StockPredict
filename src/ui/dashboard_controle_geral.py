import runpy
from pathlib import Path


def render_controle_geral():
    runpy.run_path(str(Path(__file__).with_name("dashboard_controle_geral_script.py")))
