"""Script temporário de diagnóstico — remover após uso."""
import sys, os
from pathlib import Path
print("sys.frozen:", getattr(sys, "frozen", "NOT SET"))
print("sys.executable:", sys.executable)
print("Path(sys.executable).parent:", Path(sys.executable).parent)
print("__file__:", __file__)
print("CWD:", os.getcwd())
