import pytest
from astroquery.utils.tap import Tap
from astropy.table import Table

@pytest.fixture
def gaiatap():
    return Tap("http://gea.esac.esa.int:80/tap-server/tap")

def test_load_tables(gaiatap):
    gaiatap.load_tables()

def test_load_table(gaiatap):
    gaiatap.load_table('gaiadr2.gaia_source')

def test_launch_job(gaiatap):
    query = 'select top 5 * from gaiadr1.tgas_source;'
    r = gaiatap.launch_job(query)

    query = 'select top 5 * from TAP_UPLOAD.mytable;'
    mytable = Table({'a':[1,2,3], 'b':[4,5,6]})
    r = gaiatap.launch_job(query, upload_resource=mytable, upload_table_name='mytable')


def test_tap_from_url():
    tap = Tap.from_url("http://gea.esac.esa.int:80/tap-server/tap")
    assert tap.protocol == 'http', "Tap has a wrong protocol"
    assert tap.host == 'gea.esac.esa.int', "Tap has a wrong host"
    assert tap.port == 80, "Tap has a wrong port"
    assert tap.server_context == '/tap-server', "Tap has a server_context"
    assert tap.tap_context == 'tap', "Tap has a tap_context"