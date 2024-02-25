from abc import ABC, abstractmethod
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.query import Result
from rdflib.plugins.stores.sparqlstore import SPARQLStore, SPARQLUpdateStore
from typing import Optional
from datetime import datetime
from pyrdfstore.common import QUERY_BUILDER
import logging


log = logging.getLogger(__name__)
ADMIN_NAMED_GRAPH = "urn:py-rdf-store:admin"
SCHEMA = Namespace("https://schema.org/")
SCHEMA_DATEMODIFIED = SCHEMA.dateModified


def timestamp():
    return datetime.utcnow()


class RDFStore(ABC):
    @abstractmethod
    # TODO -- consider allowing here to slide in a named_graph also -- narrowing down the select to that scope
    def select(self, sparql: str) -> Result:
        pass

    @abstractmethod
    def insert(self, graph: Graph, named_graph: Optional[str] = None) -> None:
        pass

    def verify_max_age(self, named_graph: str, age_minutes: int) -> bool:
        named_graph_lastmod = self.lastmod_for_named_graph(named_graph)
        if named_graph_lastmod is None:
            return False
        ts = timestamp()
        return bool(
            (ts - named_graph_lastmod).total_seconds() <= age_minutes * 60
        )

    @abstractmethod
    def lastmod_for_named_graph(self, named_graph: str) -> datetime:
        pass

    @abstractmethod
    def drop_graph(self, named_graph: str) -> None:
        pass


class URIRDFStore(RDFStore):
    """ "
        This class is used to connect to a SPARQL endpoint and execute SPARQL queries

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
            self.sparql_store = SPARQLUpdateStore(query_endpoint=read_uri, update_endpoint=write_uri, method='POST', autocommit=True)
            self.allows_update = True
        self._qryBuilder = QUERY_BUILDER

    def select(self, sparql: str) -> Result:
        log.debug(f"exec select {sparql=}")
        result: Result = self.sparql_store.query(query=sparql)
        log.debug(f"from SPARQLStore :: {type(result)=} -> {result=}")
        return result

    def insert(self, graph: Graph, named_graph: Optional[str] = None):
        assert self.allows_update, "data can not be inserted into a store if no write_uri is provided"
        #self.sparql_store.add_graph(graph)
        store_graph = Graph(store=self.sparql_store, identifier=named_graph)
        store_graph += graph

        if named_graph is not None and len(named_graph) > 0:
            self._update_registry_lastmod(named_graph, timestamp())

    def _update_registry_lastmod(self, named_graph: str, lastmod: datetime = None) -> None:
        graph_subject = URIRef(named_graph)

        store_graph = Graph(store=self.sparql_store, identifier=ADMIN_NAMED_GRAPH)
        # remove any previous triple for this graph
        pattern = tuple((graph_subject, SCHEMA_DATEMODIFIED, None))  # missing object functions as pattern
        store_graph.remove(pattern)
        # and insert the new one if provided
        if lastmod is None:
            return
        # else
        triple = tuple((graph_subject, SCHEMA_DATEMODIFIED, Literal(lastmod)))
        store_graph.add(triple)

    def lastmod_for_named_graph(self, named_graph: str) -> datetime:
        store_graph = Graph(store=self.sparql_store, identifier=ADMIN_NAMED_GRAPH)
        lastmod: Literal = store_graph.value(URIRef(named_graph), SCHEMA_DATEMODIFIED)
        # above is None if nothing found, else convert the literal to actual .value (datetime)
        return lastmod.value if lastmod is not None else None

    def drop_graph(self, named_graph: str) -> None:
        store_graph = Graph(store=self.sparql_store, identifier=named_graph)
        self.sparql_store.remove_graph(store_graph)
        self._update_registry_lastmod(named_graph)


class MemoryRDFStore(RDFStore):
    # check if rdflib.Dataset could not help out here, such would allign more logically and elegantly?
    def __init__(self):
        self._all: Graph = Graph()
        self._named_graphs = dict()
        self._admin_registry = dict()

    def select(self, sparql: str) -> Result:
        return self._all.query(sparql)

    def insert(self, graph: Graph, named_graph: Optional[str] = None):
        named_graph_graph = None
        if named_graph is not None:
            if named_graph not in self._named_graphs:
                self._named_graphs[named_graph] = Graph()
            named_graph_graph: Graph = self._named_graphs[named_graph]
            named_graph_graph += graph
            self._admin_registry[named_graph] = timestamp()
        self._all += graph

    def lastmod_for_named_graph(self, named_graph: str) -> datetime:
        return self._admin_registry[named_graph]

    def drop_graph(self, named_graph: str) -> None:
        self._all -= self._named_graphs[named_graph]
        self._named_graphs.pop(named_graph, None)
        self._admin_registry.pop(named_graph, None)
