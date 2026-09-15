#! /usr/bin/env python3
"""Deprecated 4.0 preview import path. Prefer ``import TSVZ``.

The 3.39 API is ``import TSVZ_old``. This alias will be removed in TSVZ 5.0.
"""
import warnings

warnings.warn(
	'TSVZ_new is the 4.0 preview import path; use `import TSVZ`. '
	'This alias will be removed in TSVZ 5.0. '
	'The 3.39 API is `import TSVZ_old`.',
	DeprecationWarning,
	stacklevel=2,
)
from TSVZ import *  # noqa: F403, E402
from TSVZ import (  # noqa: E402, F401
	BREAKING_CHANGES_4_0, __version__, version, __all__,
)
