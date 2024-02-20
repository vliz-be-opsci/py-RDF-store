import pytest
import os
from pyrdfstore.store import URIRDFStore, MemoryRDFStore
from rdflib import Graph
from pyrdfj2 import J2RDFSyntaxBuilder
from pyrdfstore.common import QUERY_BUILDER


@pytest.fixture()
def rdf_store():
    read_uri = os.getenv("TEST_SPARQL_READ_URI")
    write_uri = os.getenv("TEST_SPARQL_WRITE_URI")
    if read_uri is not None:
        return URIRDFStore(QUERY_BUILDER, read_uri, write_uri)
    # else
    return MemoryRDFStore()


@pytest.fixture()
def prepopulated_rdf_store(rdf_store):
    graph = Graph()
    graph.parse("tests/input/3293.jsonld", format="json-ld")
    rdf_store.insert(graph)
    return rdf_store
