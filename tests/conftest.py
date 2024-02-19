import pytest
import os
from pyrdfstore.store import URITargetStore, MemoryTargetStore
from rdflib import Graph
from pyrdfj2 import J2RDFSyntaxBuilder
from pyrdfstore.common import QUERY_BUILDER


@pytest.fixture()
def target_store():
    read_uri = os.getenv("TEST_SPARQL_READ_URI")
    write_uri = os.getenv("TEST_SPARQL_WRITE_URI")
    if read_uri is not None:
        return URITargetStore(QUERY_BUILDER, read_uri, write_uri)
    # else
    return MemoryTargetStore()


@pytest.fixture()
def prepopulated_target_store(target_store):
    graph = Graph()
    graph.parse("tests/input/3293.jsonld", format="json-ld")
    target_store.insert(graph)
    return target_store
