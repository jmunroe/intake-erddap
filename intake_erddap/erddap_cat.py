
from . import __version__
from intake.catalog.base import Catalog
from intake.catalog.local import LocalCatalogEntry
from erddapy import ERDDAP


class ERDDAPCatalog(Catalog):
    """
    Makes data sources out of all datasets the given ERDDAP service

    This uses erddapy to infer the datasets on the target server.
    Of these, those which have at least one primary key column will become
    ``ERDDAPSourceAutoPartition`` entries in this catalog.
    """
    name = 'erddap_cat'
    version = __version__

    def __init__(self, server, **kwargs):
        self.server = server
        super(ERDDAPCatalog, self).__init__(**kwargs)

    def _load(self):

        from intake_erddap import ERDDAPSource, ERDDAPSourceAutoPartition

        e = ERDDAP(self.server)
        e.protocol = 'tabledap'
        e.dataset_id = 'allDatasets'
        
        df = e.to_pandas()

        self._entries = {}

        for index, row in df.iterrows():
            dataset_id = row['datasetID']
            if dataset_id == 'allDatasets':
                continue

            description = 'ERDDAP dataset_id %s from %s' % (dataset_id, self.server)
            args = {'server': self.server,
                    'dataset_id': dataset_id,
                    'protocol': 'tabledap', 
                    }

            if False: # if we can use AutoPartition
                e = LocalCatalogEntry(dataset_id, description, 'erddap_auto', True,
                                        args, {}, {}, {}, "", getenv=False,
                                        getshell=False)
                e._plugin = [ERDDAPSourceAutoPartition]
            else: # if we can't use AutoPartition
                e = LocalCatalogEntry(dataset_id, description, 'erddap', True,
                                      args, {}, {}, {}, "", getenv=False,
                                      getshell=False)
                e._plugin = [ERDDAPSource]
            
            self._entries[dataset_id] = e
