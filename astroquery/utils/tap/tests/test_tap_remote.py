import pytest
from astroquery.utils.tap import Tap

@pytest.fixture
def gaiatap():
    return Tap("https://gea.esac.esa.int:80/tap-server/tap")

def test_load_tables(gaiatap):
    gaiatap.load_tables()

def test_load_table(gaiatap):
    gaiatap.load_table('gaiadr2.gaia_source')