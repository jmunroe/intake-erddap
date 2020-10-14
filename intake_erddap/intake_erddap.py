from intake.source import base
from . import __version__
from erddapy import ERDDAP


class ERDDAPSource(base.DataSource):
    """
    One-shot ERDDAP to dataframe reader (no partitioning)
    (TableDAP only)

    Caches entire dataframe in memory.

    Parameters
    ----------
    server: str
        URI to ERDDAP server
    dataset_id: str

    variables: list

    constraints: dict

    """
    name = 'erddap'
    version = __version__
    container = 'dataframe'
    partition_access = True

    def __init__(self, server, dataset_id, 
                protocol='tabledap', 
                variables=[], constraints={}, metadata={}):
        self._init_args = {
            'server': server,
            'dataset_id': dataset_id,
            'protocol': protocol,
            'variables': variables,
            'constraints': constraints,
            'metadata': metadata,
        }

        self._server = server
        self._dataset_id = dataset_id
        self._protocol = protocol
        self._variables = variables
        self._constraints = constraints
        self._dataframe = None

        super(ERDDAPSource, self).__init__(metadata=metadata)

    def _load(self):

        e = ERDDAP(server=self._server)
        e.protocol = self._protocol
        e.dataset_id = self._dataset_id
        e.variables = self._variables
        e.constrants = self._constraints

        self._dataframe = e.to_pandas()
        

    def _get_schema(self):
        if self._dataframe is None:
            # TODO: could do partial read with chunksize to get likely schema from
            # first few records, rather than loading the whole thing
            self._load()
        return base.Schema(datashape=None,
                           dtype=self._dataframe.dtypes,
                           shape=self._dataframe.shape,
                           npartitions=1,
                           extra_metadata={})

    def _get_partition(self, _):
        if self._dataframe is None:
            self._load_metadata()
        return self._dataframe

    def read(self):
        return self._get_partition(None)

    def _close(self):
        self._dataframe = None


class ERDDAPSourceAutoPartition(base.DataSource):
    """
    ERDDAP TableDAP reader with automatic partitioning

    For partitioning, require to provide the variable to be used, which should
    be indexed in the database. Can then provide list of boundaries, number
    of partitions or target partition size; see dask.dataframe.read_sql_table
    and examples for a list of possibilities.

    Parameters
    ----------
    uri: str or None
        Full connection string in sqlalchemy syntax
    table: str
        Table to read
    index: str
        Column to use for partitioning and as the index of the resulting
        dataframe
    sql_kwargs: dict
        Further arguments to pass to dask.dataframe.read_sql
    """
    name = 'erddap_auto'
    version = __version__
    container = 'dataframe'
    partition_access = True

    def __init__(self, uri, table, index, sql_kwargs={}, metadata={}):
        self._init_args = {
            'uri': uri,
            'sql_expr': table,
            'index': index,
            'sql_kwargs': sql_kwargs,
            'metadata': metadata,
        }

        self._uri = uri
        self._sql_expr = table
        self._sql_kwargs = sql_kwargs
        self._index = index
        self._dataframe = None

        super(ERDDAPSourceAutoPartition, self).__init__(metadata=metadata)

    def _load(self):
        import dask.dataframe as dd
        self._dataframe = dd.read_sql_table(self._sql_expr, self._uri,
                                            self._index, **self._sql_kwargs)

    def _get_schema(self):
        if self._dataframe is None:
            self._load()
        return base.Schema(datashape=None,
                           dtype=self._dataframe,
                           shape=(None, len(self._dataframe.columns)),
                           npartitions=self._dataframe.npartitions,
                           extra_metadata={})

    def _get_partition(self, i):
        if self._dataframe is None:
            self._load_metadata()
        return self._dataframe.get_partition(i).compute()

    def to_dask(self):
        self._get_schema()
        return self._dataframe
    
    def read(self):
        self._get_schema()
        return self._dataframe.compute()

    def _close(self):
        self._dataframe = None


class ERDDAPSourceManualPartition(base.DataSource):
    """
    ERDDAP expression reader with explicit partitioning

    Reads any arbitrary SQL expressions into partitioned data-frame, but
    requires a full specification of the boundaries.

    The boundaries are specified as either a set of strings with `WHERE`
    clauses to be applied to the main SQL expression, or a string to be
    formatted with a set of values to produce the comlete SQL expressions.

    Note, if not supplying a `meta` argument, dask will load the first
    partition in order to determine the schema. If some of the partitions are
    empty, loading without a meta will likely fail.

    Parameters
    ----------
    uri: str or None
        Full connection string in sqlalchemy syntax
    sql_expr: str
        SQL expression to evaluate
    where_values: list of str or list of values/tuples
        Either a set of explicit partitioning statements (e.g.,
        `"WHERE index_col < 50"`...) or pairs of valued to be entered into
        where_template, if using
    where_template: str (optional)
        Template for generating partition selection clauses, using the
        values from where_values, e.g.,
        `"WHERE index_col >= {} AND index_col < {}"`
    sql_kwargs: dict
        Further arguments to pass to pd.read_sql_query
    """
    name = 'erddap_manual'
    version = __version__
    container = 'dataframe'
    partition_access = True

    def __init__(self, uri, sql_expr, where_values, where_template=None,
                 sql_kwargs={}, metadata={}):
        self._init_args = {
            'uri': uri,
            'sql_expr': sql_expr,
            'where': where_values,
            'where_tmp': where_template,
            'sql_kwargs': sql_kwargs,
            'metadata': metadata,
        }

        self._uri = uri
        self._sql_expr = sql_expr  # TODO: may check for table and expand to
                                   # "SELECT * FROM {table}"
        self._sql_kwargs = sql_kwargs
        self._where = where_values
        self._where_tmp = where_template
        self._dataframe = None
        self._meta = self._sql_kwargs.pop('meta', None)

        super(SQLSourceManualPartition, self).__init__(metadata=metadata)

    def _load(self):
        self._dataframe = read_sql_query(self._uri, self._sql_expr,
                                         self._where, where_tmp=self._where_tmp,
                                         meta=self._meta,
                                         kwargs=self._sql_kwargs)

    def _get_schema(self):
        if self._dataframe is None:
            self._load()
        return base.Schema(datashape=None,
                           dtype=self._dataframe,
                           shape=(None, len(self._dataframe.columns)),
                           npartitions=self._dataframe.npartitions,
                           extra_metadata={})

    def _get_partition(self, i):
        if self._dataframe is None:
            self._load_metadata()
        return self._dataframe.get_partition(i).compute()

    def to_dask(self):
        self._get_schema()
        return self._dataframe

    def read(self):
        self._get_schema()
        return self._dataframe.compute()

    def _close(self):
        self._dataframe = None


def load_part(sql, engine, where, kwargs, meta=None):
    import pandas as pd
    sql = sql + ' ' + where
    df = pd.read_sql(sql, engine, **kwargs)
    if meta is not None:
        if df.empty:
            df = meta
        else:
            df = df.astype(meta.dtypes.to_dict(), copy=False)
    return df


def read_sql_query(uri, sql, where, where_tmp=None, meta=None, kwargs=None):
    """
    Create a dask dataframe from ERDDAP using explicit partitioning

    Parameters
    ----------
    uri: str
        connection string (sql sqlalchemy documentation)
    sql: str
        SQL query to execute
    where: list of str or list of tuple
        Either a set of explicit partitioning statements (e.g.,
        `"WHERE index_col < 50"`...) or pairs of valued to be entered into
        where_template, if using
    where_tmp: str (optional)
        Template for generating partition selection clauses, using the
        values from where_values, e.g.,
        `"WHERE index_col >= {} AND index_col < {}"`
    meta: dataframe metadata (optional)
        If given, a zero-length version of the dataframe structure, with
        index and column names and types correctly specified. Can also be
        the same information in dictionary or tuple of tuples format
    kwargs: dict
        Any further parameters to pass to pd.read_sql_query, see
        its documentation
    """
    import dask
    import dask.dataframe as dd
    if where_tmp is not None:
        where = [where_tmp.format(values) for values in where]
    if kwargs is None:
        kwargs = {}
    dload = dask.delayed(load_part)
    parts = [dload(sql, uri, w, kwargs) for w in where]
    return dd.from_delayed(parts, meta=meta)
