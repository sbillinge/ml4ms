import json
import os


class NotSpecifiedType(object):
    """A helper class singleton for run control meaning that a 'real' value
    has not been given."""

    def __repr__(self):
        return "NotSpecified"


NotSpecified = NotSpecifiedType()
"""A helper class singleton for run control meaning that a 'real' value
has not been given.
"""


class RunControl(object):
    """A composable configuration class. Unlike argparse.Namespace,
    this keeps the object dictionary (__dict__) separate from the run
    control attributes dictionary (_dict).
    """

    def __init__(self, _updaters=None, _validators=None, **kwargs):
        """Parameters
        -------------
        kwargs : optional
            Items to place into run control.

        """
        self._dict = {}
        self._updaters = _updaters or {}
        self._validators = _validators or {}
        for k, v in kwargs.items():
            setattr(self, k, v)

    def _update(self, other):
        """Updates the rc with values from another mapping.  If this rc has
        if a key is in self, other, and self._updaters, then the updaters
        value is called to perform the update.  This function should return
        a copy to be safe and not update in-place.
        """
        if hasattr(other, "_dict"):
            other = other._dict
        elif not hasattr(other, "items"):
            other = dict(other)
        for k, v in other.items():
            if v is NotSpecified:
                pass
            elif k in self._updaters and k in self:
                v = self._updaters[k](getattr(self, k), v)
            setattr(self, k, v)


def load_json_rcfile(fname):
    """Loads a JSON run control file."""
    with open(fname, "r", encoding="utf-8") as f:
        rc = json.load(f)
    return rc


def load_rcfile(fname):
    """Loads a run control file."""
    base, ext = os.path.splitext(fname)
    if ext == ".json":
        rc = load_json_rcfile(fname)
    else:
        raise RuntimeError("could not determine run control file type from extension.")
    return rc


DEFAULT_RC = RunControl(database=[], user_config=os.path.expanduser("~/.config/ml4ms/user.json"))

#
#  {'databases': [
#     {"name": "default",
#      "url": ".",
#      "public": False,
#      "path": "./db",
#      "local": True}
# ]}
