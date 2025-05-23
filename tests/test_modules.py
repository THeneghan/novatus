"""Tests for modules."""

import os
import pkgutil
from importlib import import_module
from pathlib import Path


def test_loading_src_modules():
    """Imports all modules in src to check for any failed imports and any runtime errors"""
    novatus_path = Path(__file__).parent.parent
    src_path = os.path.join(novatus_path, "src")
    if not Path(src_path).exists():
        raise NotADirectoryError
    for module in pkgutil.walk_packages(path=[src_path]):
        import_module(module.name)
