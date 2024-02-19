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


class TargetStore(ABC):
    @abstractmethod
    def select(self, sparql: str) -> SPARQLResult:
        pass

    @abstractmethod
    def insert(self, graph: Graph, context: Optional[str] = None) -> None:
        pass

    def verify_max_age(self, context: str, age_minutes: int) -> bool:
        context_lastmod = self.lastmod_for_context(context)
        ts = datetime.utcnow()
        return bool((ts - context_lastmod).total_seconds() <= age_minutes * 60)

    @abstractmethod
    def lastmod_for_context(self, context: str) -> datetime:
        pass


class URITargetStore(TargetStore):
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

    def insert(self, graph: Graph, context: Optional[str] = None):
        # TODO push the graph into the store - and manage the context
        batches = URITargetStore._graph_to_batches(graph)

        for batch in batches:
            vars = {"context": context, "raw_triples": batch}
            query = self._qryBuilder.build_syntax(
                "insert_graph.sparql", **vars
            )
            self.client.setQuery(query)
            self.client.query()

            lastmod = timestamp()
            self._update_registry_lastmod(lastmod, context)

    def _update_registry_lastmod(self, lastmod: str, context: str):
        vars = {
            "context": context,
            "lastmod": lastmod,
            "registry_of_lastmod_context": "urn:PYTHONRDFSTORECLIENT:ADMIN",
        }

        query = self._qryBuilder.build_syntax(
            "update_registry_lastmod.sparql", **vars
        )

        self.client.setQuery(query)
        self.client.query()

    def lastmod_for_context(self, context: str) -> datetime:
        vars = {
            "registry_of_lastmod_context": "urn:PYTHONRDFSTORECLIENT:ADMIN",
        }
        query = self._qryBuilder.build_syntax("lastmod_info.sparql", **vars)

        self.client.setQuery(query)
        result = self.client.query().convert()
        all_results = URITargetStore._convert_result_to_datetime(result)
        return all_results[context]

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


class MemoryTargetStore(TargetStore):
    def __init__(self):
        self._all: Graph = Graph()
        self._named_graphs = dict()
        self._admin_registry = dict()

    def select(self, sparql: str) -> SPARQLResult:
        return self._all.query(sparql)

    def insert(self, graph: Graph, context: Optional[str] = None):
        context_graph = None
        if context is not None:
            if context not in self._named_graphs:
                self._named_graphs[context] = Graph()
            context_graph: Graph = self._named_graphs[context]
            context_graph += graph
            self._admin_registry[context] = timestamp()
        self._all += graph

    def lastmod_for_context(self, context: str) -> datetime:
        return self._admin_registry[context]


class TargetStoreAccess:

    def __init__(self, target: TargetStore, qryBuilder: J2RDFSyntaxBuilder):
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

    def ingest(self, graph: Graph, context: str):
        self._target.insert(graph, context)

    # def lastmod_for_context(self, context: str) -> datetime:
    #    return self._target.lastmod_for_context(context)
