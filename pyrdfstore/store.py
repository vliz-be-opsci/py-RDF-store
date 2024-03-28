import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from datetime import datetime
from typing import Optional

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLStore, SPARQLUpdateStore
from rdflib.query import Result

log = logging.getLogger(__name__)

NIL_NS = "urn:_:nil"
ADMIN_NAMED_GRAPH = "urn:py-rdf-store:admin"
SCHEMA = Namespace("https://schema.org/")
SCHEMA_DATEMODIFIED = SCHEMA.dateModified


def timestamp():
    return datetime.utcnow()


class RDFStore(ABC):
    """This interface describes the basic contract for having read, write operations versus a
    managed set of named-graphs so that the lastmod timestamp on each of these is being tracked properly
    so the 'age' of these can be compared easily to decide on required or oportune updates
    """

    @abstractmethod
    def select(self, sparql: str, named_graph: Optional[str]) -> Result:
        """executes a sparql select query, possibly narrowed to the named_grap it represents

        :param sparql: the query-statement to execute
        :type sparql: str
        :param named_graph: the uri describing the named_graph into which the select should be narrowed
        :type named_graph: str
        :return: the result of the query
        :rtype: Result
        """
        pass

    @abstractmethod
    def insert(self, graph: Graph, named_graph: Optional[str] = None) -> None:
        """inserts the triples from the passed graph into the suggested named_graph

        :param graph: the graph of triples to insert
        :type graph: Graph
        :param named_graph: the uri describing the named_graph into which the graph should be inserted
        :type named_graph: str
        :rtype: None
        """
        pass

    def verify_max_age(self, named_graph: str, age_minutes: int) -> bool:
        """verifies that a certain graph is not aged older than a certain amount of minutes
        (as this just uses self.lastmod_ts() implementations should just get that right and simply inherit this)

        :param named_graph: the uri describing the named_graph to check the age of
        :type named_graph: str
        :param age_minutes: the max acceptable age in minutes
        :type age_minutes: int
        :return: True if the graph has aged less than the passed number of minutes in the argument, else False
        :rtype: bool
        """
        named_graph_lastmod = self.lastmod_ts(named_graph)
        if named_graph_lastmod is None:
            return False
        ts = timestamp()
        return bool(
            (ts - named_graph_lastmod).total_seconds() <= age_minutes * 60
        )

    @abstractmethod
    def lastmod_ts(self, named_graph: str) -> datetime:
        """returns the update timestamp of the specified graph

        :param named_graph: the uri describing the named_graph to get the lastmod timestamp of
        :type named_graph: str
        :return: the time of last modification (insert or drop)
        :rtype: datetime
        """
        pass

    @property
    @abstractmethod
    def named_graphs(self) -> Iterable[str]:
        """returns the known & managed named-graphs in the store

        :return: the list of named-graphs, known and managed (possibly already deleted) in this store
        :rtype: List[str]
        """
        pass

    @abstractmethod
    def drop_graph(self, named_graph: str) -> None:
        """drops the specifed named_graph (and all its contents)
        Note: dropping any unknown graph should just work without complaints
        Note: dropping a graph still leaves a trail of its 'update'
              in the admin-graph (meaning its age can be verified)
              Consider forget_graph to remove that trail of 'update'

        :param named_graph: the uri describing the named_graph to drop
        :type named_graph: str
        :rtype: None
        """
        pass

    @abstractmethod
    def forget_graph(self, named_graph: str) -> None:
        """forgets about the names_graph being under control
        This functions independent of the drop_graph method.
        So any client of this service is expected to decide when (or not) to combine both

        Note: dropping any unknown graph should just work without complaints
        Note: forgetting a graph removes any trail of its 'update'
              in the admin-graph.
              It does nothing to also actually removed the content in the graph

        :param named_graph: the uri describing the named_graph to drop
        :type named_graph: str
        :rtype: None
        """
        pass


class URIRDFStore(RDFStore):
    """This class is used to connect to a SPARQL endpoint and execute SPARQL queries

    :param read_uri: The URI of the SPARQL endpoint to read from
    :type read_uri: str
    :param write_uri: The URI of the SPARQL endpoint to write to.
                        If not provided, the store can only be read from, not updated.
    :type write_uri: Optional[str]
    """

    def __init__(self, read_uri: str, write_uri: Optional[str] = None):
        self.sparql_store = None
        self.allows_update = False
        if write_uri is None:
            self.sparql_store = SPARQLStore(query_endpoint=read_uri)
        else:
            self.sparql_store = SPARQLUpdateStore(
                query_endpoint=read_uri,
                update_endpoint=write_uri,
                method="POST",
                autocommit=True,
            )
            self.allows_update = True

    def select(self, sparql: str, named_graph: Optional[str] = None) -> Result:
        log.debug(f"exec select {sparql=} into {named_graph=}")
        if named_graph is not None:
            select_graph = Graph(
                store=self.sparql_store, identifier=named_graph
            )
        else:
            select_graph = Graph(store=self.sparql_store)
        result: Result = select_graph.query(sparql)
        assert isinstance(result, Result), (
            f"Failed getting proper result for:" "{sparql=}, got {result=}"
        )
        log.debug(f"Result from SPARQLStore :: {type(result)=} -> {result=}")
        return result

    def insert(self, graph: Graph, named_graph: Optional[str] = NIL_NS):
        assert (
            self.allows_update
        ), "data can not be inserted into a store if no write_uri is provided"
        log.debug(f"insertion of {len(graph)=} into ({named_graph=})")
        store_graph = Graph(store=self.sparql_store, identifier=named_graph)
        store_graph += graph.skolemize()
        self._update_registry_lastmod(named_graph, timestamp())

    def _update_registry_lastmod(
        self, named_graph: str, lastmod: datetime = None
    ) -> Iterable[str]:
        """Consults and changes the admin-graph of lastmod entries per named_graph.

        :param named_graph: the named_graph to be handled, required, can be None to return the list of all available names
        :type named_graph: str (or None)
        :param lastmod: the new lastmod timestamp for this named_graph, if None (or not provided) this will 'forget' the named_graph
        :type lastmod: datetime
        :return: the list of named_graphs in management
        :rtype: Iterable[str]
        """
        graph_subject = (
            URIRef(named_graph) if named_graph is not None else None
        )
        response = [named_graph] if named_graph is not None else None

        adm_graph = Graph(
            store=self.sparql_store, identifier=ADMIN_NAMED_GRAPH
        )

        # construct what we are matching for
        pattern = tuple(
            (graph_subject, SCHEMA_DATEMODIFIED, None)
        )  # missing subject or object functions as pattern
        # remove any previous triple for this graph if it is specified
        if graph_subject is not None:
            adm_graph.remove(pattern)
        else:
            response = [
                str(sub) for (sub, pred, obj) in adm_graph.triples(pattern)
            ]

        # insert the new data if provided
        if graph_subject is not None and lastmod is not None:
            triple = tuple(
                (graph_subject, SCHEMA_DATEMODIFIED, Literal(lastmod))
            )
            adm_graph.add(triple)

        return response

    def lastmod_ts(self, named_graph: str) -> datetime:
        adm_graph = Graph(
            store=self.sparql_store, identifier=ADMIN_NAMED_GRAPH
        )
        lastmod: Literal = adm_graph.value(
            URIRef(named_graph), SCHEMA_DATEMODIFIED
        )
        # above is None if nothing found, else convert the literal to actual .value (datetime)
        return lastmod.value if lastmod is not None else None

    def drop_graph(self, named_graph: str) -> None:
        store_graph = Graph(store=self.sparql_store, identifier=named_graph)
        self.sparql_store.remove_graph(store_graph)
        self._update_registry_lastmod(named_graph, timestamp())

    @property
    def named_graphs(self) -> Iterable[str]:
        return self._update_registry_lastmod(None)

    def forget_graph(self, named_graph: str) -> None:
        self._update_registry_lastmod(named_graph, None)


class MemoryRDFStore(RDFStore):
    # check if rdflib.Dataset could not help out here, such would allign more logically and elegantly?
    def __init__(self):
        self._all: Graph = Graph()
        self._named_graphs = dict()
        self._admin_registry = dict()

    def select(self, sparql: str, named_graph: Optional[str] = None) -> Result:
        target = (
            self._named_graphs[named_graph]
            if named_graph is not None
            else self._all
        )
        return target.query(sparql)

    def insert(self, graph: Graph, named_graph: Optional[str] = None):
        named_graph_graph = None
        if named_graph is not None:
            if named_graph not in self._named_graphs:
                self._named_graphs[named_graph] = Graph()
            named_graph_graph: Graph = self._named_graphs[named_graph]
            named_graph_graph += graph
            self._admin_registry[named_graph] = timestamp()
        self._all += graph

    def lastmod_ts(self, named_graph: str) -> datetime:
        return self._admin_registry[named_graph]

    def drop_graph(self, named_graph: str) -> None:
        if named_graph is not None and named_graph in self._named_graphs:
            self._all -= self._named_graphs.pop(named_graph)
        self._admin_registry[named_graph] = timestamp()

    @property
    def named_graphs(self) -> Iterable[str]:
        return self._admin_registry.keys()

    def forget_graph(self, named_graph: str) -> None:
        self._admin_registry.pop(named_graph)
