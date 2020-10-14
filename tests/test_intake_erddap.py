import intake
from intake_erddap import (ERDDAPSourceAutoPartition, 
                           ERDDAPSourceManualPartition,
                           ERDDAPSource)
from .utils import df, df2
import pandas as pd

# pytest imports this package last, so plugin is not auto-added
intake.registry['erddap'] = ERDDAPSource
intake.registry['erddap_auto'] = ERDDAPSourceAutoPartition
intake.registry['erddap_manual'] = ERDDAPSourceManualPartition


def test_fixture():
    # how to "fake" an ERDDAP server for testing purposes?
    assert False
    

def test_simple():
    server = 'https://cioosatlantic.ca/erddap'
    dataset_id = 'SMA_bay_of_exploits'

    d2 = ERDDAPSource(server, dataset_id).read()
    
    print(len(d2))
    assert len(d2) > 0


def test_auto():
    server = 'https://cioosatlantic.ca/erddap'
    dataset_id = 'SMA_bay_of_exploits'
    
    
    assert False

    table, table_nopk, uri = temp_db
    s = ERDDAPSourceAutoPartition(uri, table, index='p',
                               sql_kwargs=dict(npartitions=2))
    assert s.discover()['npartitions'] == 2
    assert s.to_dask().npartitions == 2
    d2 = s.read()
    assert df.equals(d2)


def test_manual():
    assert False

    table, table_nopk, uri = temp_db
    s = ERDDAPSourceManualPartition(uri, "SELECT * FROM " + table,
               where_values=['WHERE p < 20', 'WHERE p >= 20'],
               sql_kwargs=dict(index_col='p'))
    assert s.discover()['npartitions'] == 2
    assert s.to_dask().npartitions == 2
    d2 = s.read()
    assert df.equals(d2)
