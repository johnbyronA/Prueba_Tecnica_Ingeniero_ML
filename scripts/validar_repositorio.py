from __future__ import annotations

import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = [
    ROOT / "README.md",
    ROOT / "requirements.txt",
    ROOT / "config" / "parametros_ejemplo.yml",
    ROOT / "entregables" / "presentacion_prueba_tecnica_mlops.pptx",
]

FORBIDDEN_TEXT = [
    "johnbyronh321@gmail.com",
    "dbc-1964c799-3e30.cloud.databricks.com",
    "7474659037154462",
]


def require_files() -> None:
    missing = [str(path.relative_to(ROOT)) for path in REQUIRED_FILES if not path.exists()]
    if missing:
        raise AssertionError(f"Faltan archivos requeridos: {missing}")


def validate_notebooks() -> None:
    notebooks = sorted((ROOT / "notebooks").glob("*.ipynb"))
    if len(notebooks) != 7:
        raise AssertionError(f"Se esperaban 7 notebooks y se encontraron {len(notebooks)}")

    for path in notebooks:
        with path.open(encoding="utf-8-sig") as stream:
            notebook = json.load(stream)
        if "cells" not in notebook or "nbformat" not in notebook:
            raise AssertionError(f"Notebook inválido: {path.name}")


def validate_source_exports() -> None:
    sources = sorted((ROOT / "src" / "databricks").glob("*.py"))
    if len(sources) != 7:
        raise AssertionError(f"Se esperaban 7 fuentes Databricks y se encontraron {len(sources)}")
    for path in sources:
        first_line = path.read_text(encoding="utf-8").splitlines()[0]
        if first_line != "# Databricks notebook source":
            raise AssertionError(f"Formato Databricks inválido: {path.name}")


def validate_anonymization() -> None:
    roots = [ROOT / "notebooks", ROOT / "src" / "databricks", ROOT / "evidencias" / "notebooks_html"]
    for folder in roots:
        for path in folder.iterdir():
            if not path.is_file():
                continue
            content = path.read_text(encoding="utf-8-sig", errors="ignore")
            for forbidden in FORBIDDEN_TEXT:
                if forbidden in content:
                    raise AssertionError(f"Dato interno sin anonimizar en {path.relative_to(ROOT)}")


def validate_csv(path: Path, expected_columns: set[str]) -> None:
    with path.open(newline="", encoding="utf-8-sig") as stream:
        columns = set(csv.DictReader(stream).fieldnames or [])
    if not expected_columns.issubset(columns):
        raise AssertionError(f"Columnas incompletas en {path.name}: {columns}")


def main() -> None:
    require_files()
    validate_notebooks()
    validate_source_exports()
    validate_anonymization()
    validate_csv(
        ROOT / "evidencias" / "resultados" / "metricas_modelos.csv",
        {"model", "roc_auc", "area_under_pr"},
    )
    validate_csv(
        ROOT / "evidencias" / "resultados" / "lift_por_decil.csv",
        {"decil", "casos", "fraudes", "tasa_fraude", "lift"},
    )
    print("Repositorio validado correctamente.")


if __name__ == "__main__":
    main()
