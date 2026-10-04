"""Validate notebook syntax; data contracts have separate integration tests."""
from pathlib import Path
import nbformat

root = Path(__file__).resolve().parents[1]
if not (root / "notebooks").exists():
    root = Path("/workspace")
for name in ("01_ingesta_tpch.ipynb", "01_ingesta_lineitem_tpch.ipynb",
             "01_ingesta_customer_tpch.ipynb", "02_bronze_to_silver_orchestrator.ipynb",
             "03_silver_to_gold_orchestrator.ipynb"):
    path = root / "notebooks" / name
    notebook = nbformat.read(path, as_version=4)
    nbformat.validate(notebook)
    for cell in notebook.cells:
        if cell.cell_type == "code":
            compile(cell.source, str(path), "exec")
    print(f"PASS {name}: nbformat and Python syntax")
