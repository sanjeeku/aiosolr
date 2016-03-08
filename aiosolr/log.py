import os
import logging


class NullHandler(logging.Handler):

    def emit(self, record):
        pass


LOG = logging.getLogger(__package__)
LOG.addHandler(NullHandler())
if os.environ.get('DEBUG_PYSOLR', '').lower() in ('true', '1'):
    LOG.setLevel(logging.DEBUG)
    LOG.addHandler(logging.StreamHandler())
