from abc import ABC, abstractmethod
from rdflib import Graph
from rdflib.plugins.sparql.processor import SPARQLResult
from typing import Optional, List
from datetime import datetime
from SPARQLWrapper import JSON, SPARQLWrapper
from pyrdfj2 import J2RDFSyntaxBuilder
import logging
from functools import reduce

log = logging.getLogger(__name__)


def timestamp():
    return datetime.utcnow().isoformat()


class RDFStore(ABC):
    @abstractmethod
    def select(self, sparql: str) -> SPARQLResult:
        pass

    @abstractmethod
    def insert(self, graph: Graph, named_graph: Optional[str] = None) -> None:
        pass

    def verify_max_age(self, named_graph: str, age_minutes: int) -> bool:
        named_graph_lastmod = self.lastmod_for_named_graph(named_graph)
        ts = datetime.utcnow()
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

    :param qryBuilder: helper for building sparql from templates
    :type qryBuilder: J2RDFSyntaxBuilder
    :param readURI: The URI of the SPARQL endpoint to read from
    :type readURI: str
    :param writeURI: The URI of the SPARQL endpoint to write to. If not provided, the readURI will be used.
    :type writeURI: Optional[str]
    """

    def __init__(
        self,
        qryBuilder: J2RDFSyntaxBuilder,
        read_uri: str,
        write_uri: Optional[str] = None,
    ):
        # Implement method to get the sparql wrapper
        self.client = SPARQLWrapper(
            endpoint=read_uri,
            updateEndpoint=write_uri or read_uri,
            returnFormat=JSON,
            agent="python-sparql-client",
        )
        self.client.method = "POST"
        self._qryBuilder = qryBuilder

    def select(self, sparql: str) -> SPARQLResult:
        self.client.setQuery(sparql)
        self.client.setReturnFormat(JSON)
        result = self.client.query().convert()
        log.debug("results_dict: {}".format(result))

        # given that a SPARQLResult object is expected, convert the result to a SPARQLResult object
        result_mapped = {
            "type_": "SELECT",
            "vars_": result["head"]["vars"],
            "bindings": result["results"]["bindings"],
            "askAnswer": None,  # Assuming the askAnswer is not available in the result
            "graph": None,  # Assuming the graph is not available in the result
        }

        result = SPARQLResult(result_mapped)
        return result

    def insert(self, graph: Graph, named_graph: Optional[str] = None):
        batches = URIRDFStore._graph_to_batches(graph)

        for batch in batches:
            vars = {"context": named_graph, "raw_triples": batch}
            query = self._qryBuilder.build_syntax(
                "insert_graph.sparql", **vars
            )
            self.client.setQuery(query)
            self.client.query()

            lastmod = timestamp()
            self._update_registry_lastmod(lastmod, named_graph)

    def _update_registry_lastmod(self, lastmod: str, named_graph: str):
        vars = {
            "context": named_graph,
            "lastmod": lastmod,
            "registry_of_lastmod_context": "urn:PYTHONRDFSTORECLIENT:ADMIN",
        }

        query = self._qryBuilder.build_syntax(
            "update_registry_lastmod.sparql", **vars
        )

        self.client.setQuery(query)
        self.client.query()

    def lastmod_for_named_graph(self, named_graph: str) -> datetime:
        vars = {
            "registry_of_lastmod_context": "urn:PYTHONRDFSTORECLIENT:ADMIN",
        }
        query = self._qryBuilder.build_syntax("lastmod_info.sparql", **vars)

        self.client.setQuery(query)
        result = self.client.query().convert()
        all_results = URIRDFStore._convert_result_to_datetime(result)
        return all_results[named_graph]

    def drop_graph(self, named_graph: str) -> None:
        vars = {"context": named_graph}
        query = self._qryBuilder.build_syntax("delete_graph.sparql", **vars)
        self.client.setQuery(query)
        self.client.query()

    @staticmethod
    def _convert_result_to_datetime(result):
        converted_results = {}
        for g in result["results"]["bindings"]:
            path = g["path"]["value"]
            time = datetime.strptime(
                g["time"]["value"], "%Y-%m-%dT%H:%M:%S.%fZ"
            )
            converted_results[path] = time
        return converted_results

    @staticmethod
    def _graph_to_batches(
        graph: Graph, max_str_size: Optional[int] = 4096
    ) -> List[str]:
        """Convert a graph into a list of strings, each of which is less than max_str_size in size.
        :param graph: The graph to be converted
        :type graph: Graph
        :param max_str_size: The maximum size (len) of each string
        :type str_size_kb: int
        :return: A list of strings, each of which is less than max_str_size.
        :rtype: List[str]
        """
        triples = graph.serialize(format="nt").split(
            "\n"
        )  # graph to triple statements
        unique_triples = list(set(triples))  # unique statements

        def regroup(groups, line):
            line = line.strip()
            if len(line) == 0:
                return groups
            assert (
                len(line) < max_str_size
            ), "single line exceeds max_batch_size"
            if (
                len(line) + len(groups[-1]) > max_str_size
            ):  # if this new line can't fit into the current last
                groups.append("")  # make a new last
            groups[-1] += ("\n" if len(groups[-1]) > 0 else "") + line
            return groups

        return reduce(regroup, unique_triples, [""])


class MemoryRDFStore(RDFStore):
    def __init__(self):
        self._all: Graph = Graph()
        self._named_graphs = dict()
        self._admin_registry = dict()

    def select(self, sparql: str) -> SPARQLResult:
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
        self._named_graphs.pop(named_graph, None)
        self._admin_registry.pop(named_graph, None)


class RDFStoreAccess:

    def __init__(self, target: RDFStore, qryBuilder: J2RDFSyntaxBuilder):
        self._target = target
        self._qryBuilder = qryBuilder

    def select_subjects(self, sparql) -> List[str]:
        result: SPARQLResult = self._target.select(sparql)
        # todo convert response into list of subjects
        list_of_subjects = [row[0] for row in result]
        return list_of_subjects

    def verify(self, subject, property_path):
        sparql = self._qryBuilder.build_syntax(
            "trajectory.sparql", subject=subject, property_path=property_path
        )
        result: SPARQLResult = self._target.select(sparql)
        return bool(len(result.bindings) > 0)

    def ingest(self, graph: Graph, named_graph: str):
        self._target.insert(graph, named_graph)

    def verify_max_age(self, named_graph: str, age_minutes: int) -> bool:
        return self._target.verify_max_age(named_graph, age_minutes)
