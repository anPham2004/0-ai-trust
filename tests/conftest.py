"""Make the deployed pipeline's ``framework`` package importable from tests.

In the deployed Databricks Git Folder, pipeline files do ``from framework.x
import y`` because the pipeline root (``pipelines/``) is on ``sys.path``.
Mirror that here so unit/contract tests can import the same production code.
"""

import sys
from pathlib import Path

PIPELINES_ROOT = Path(__file__).resolve().parents[1] / "pipelines"
if str(PIPELINES_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINES_ROOT))
