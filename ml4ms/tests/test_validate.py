import os
import sys
from io import StringIO

from ml4ms.main import main

# import pytest


def test_validate_python(make_db):
    repo = make_db
    os.chdir(repo)
    backup = sys.stdout
    sys.stdout = StringIO()
    main(args=["--validate"])
    out = sys.stdout.getvalue()
    sys.stdout.close()
    sys.stdout = backup
    assert "NO ERRORS IN DBS" in out


#
# def test_validate_python_single_col(make_db):
#     """
#     to see output from a failed test, comment out the code that rediriects stdout
#     to out and replace the assert with 'assert false'. Change it back afterwards
#     """
#     repo = make_db
#     os.chdir(repo)
#     ## to see what is failing, comment out the rows that capture and restore the
#     ## sys.stdout.
#     backup = sys.stdout
#     sys.stdout = StringIO()
#     main(["validate", "--collection", "projecta"])
#     out = sys.stdout.getvalue()
#     sys.stdout.close()
#     sys.stdout = backup
#     assert "NO ERRORS IN DBS" in out
#     # assert False
#
#
# def test_validate_bad_python(make_bad_db):
#     repo = make_bad_db
#     os.chdir(repo)
#     backup = sys.stdout
#     sys.stdout = StringIO()
#     with pytest.raises(SystemExit):
#         main(["validate"])
#     out = sys.stdout.getvalue()
#     sys.stdout.close()
#     sys.stdout = backup
#     assert "Errors found in " in out
#     assert "NO ERRORS IN DBS" not in out
