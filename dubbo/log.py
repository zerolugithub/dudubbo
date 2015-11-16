import logging
import sys

_ch = logging.StreamHandler(sys.stdout)
_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
_ch.setFormatter(_formatter)
logger = logging.Logger('dudubbo')
logger.setLevel(logging.INFO)
logger.addHandler(_ch)