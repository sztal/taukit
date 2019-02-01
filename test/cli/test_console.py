"""Unit tests for console sessions."""
import pytest
from taukit.cli.console import start_python_console


@pytest.mark.interactive
def test_start_python_console():
    import pdb; pdb.set_trace()
    start_python_console(namespace={'x': 1, 'y': 2})
