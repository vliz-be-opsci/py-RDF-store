import pytest
import os
from pyrdfstore.store import URIRDFStore, MemoryRDFStore
from pyrdfstore.build import create_rdf_store

from rdflib import Graph, URIRef


@pytest.fixture()
def rdf_store():
    read_uri = os.getenv("TEST_SPARQL_READ_URI", None)
    write_uri = os.getenv("TEST_SPARQL_WRITE_URI", None)
    return create_rdf_store(read_uri, write_uri)
    if read_uri is not None:
        return URIRDFStore(read_uri, write_uri)
    # else
    return MemoryRDFStore()


@pytest.fixture()
def prepopulated_rdf_store(rdf_store):
    graph = Graph()
    graph.parse("tests/input/3293.jsonld", format="json-ld")
    rdf_store.insert(graph)
    return rdf_store


@pytest.fixture()
def example_graphs():
    def make_ex_grph(n: int) -> Graph:
        g = Graph()
        triple = tuple(URIRef(f"https://example.org/{part}#{n}") for part in ["subject", "predicate", "object"])
        g.add(triple)
        return g
    return [make_ex_grph(i) for i in range(10)]
