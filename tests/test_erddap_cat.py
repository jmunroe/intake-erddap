from intake_erddap.erddap_cat import ERDDAPCatalog
from .utils import df, df2
import intake
import os
here = os.path.abspath(os.path.dirname(__file__))

# pytest imports this package last, so plugin is not auto-added
intake.registry['erddap_cat'] = ERDDAPCatalog


def test_cat():

    server = 'https://cioosatlantic.ca/erddap'
    
    cat = ERDDAPCatalog(server)
    
    assert True

    #table, table_nopk, uri = temp_db
    #cat = ERDDAPCatalog(uri)
    #assert table in cat
    #assert table_nopk in cat
    #d2 = getattr(cat, table).read()
    #assert df.equals(d2)
    #d_noindex = getattr(cat, table_nopk).read()
    #assert df2.equals(d_noindex)
    


def test_yaml_cat():
    assert False

    table, table_nopk, uri = temp_db
    os.environ['TEST_SQLITE_URI'] = uri  # used in catalog default
    cat = intake.Catalog(os.path.join(here, 'cat.yaml'))
    assert 'tables' in cat
    cat2 = cat.tables()
    assert isinstance(cat2, SQLCatalog)
    assert table in list(cat2)
    assert table_nopk in list(cat2)
    d2 = cat.tables.temp.read()
    assert df.equals(d2)
    d_noindex = getattr(cat.tables, table_nopk).read()
    assert df2.equals(d_noindex)
