import os
from pathlib import Path
from typing import Iterable

import pytest
from rdflib import Graph, URIRef
from util4tests import enable_test_logging, log

from pyrdfstore import RDFStore, create_rdf_store

TEST_INPUT_FOLDER = Path(__file__).parent / "./input"


enable_test_logging()  # note that this includes loading .env into os.getenv


@pytest.fixture(scope="session")
def quicktest() -> bool:
    """bool setting indicating to skip lengthy tests
    setting driven by setting env variable "QUICKTEST" to anything but 0 or ""
    """
    return bool(os.getenv("QUICKTEST", 0))


@pytest.fixture(scope="session")
def _mem_rdf_store() -> RDFStore:
    """in memory store
    uses simple dict of Graph
    """
    log.debug("creating in memory rdf store")
    return create_rdf_store()


@pytest.fixture(scope="session")
def _uri_rdf_store() -> RDFStore:
    """proxy to available graphdb store
    But only if environment variables are set and service is available
    else None (which will result in trimming it from rdf_stores fixture)
    """
    read_uri = os.getenv("TEST_SPARQL_READ_URI", None)
    write_uri = os.getenv("TEST_SPARQL_WRITE_URI", read_uri)
    # if no URI provided - skip this by returning None
    if read_uri is None or write_uri is None:
        log.debug("not creating uri rdf store in test - no uri provided")
        return None
    # else -- all is well
    log.debug(f"creating uri rdf store proxy to ({read_uri=}, {write_uri=})")
    return create_rdf_store(read_uri, write_uri)


@pytest.fixture()
def rdf_stores(_mem_rdf_store, _uri_rdf_store) -> Iterable[RDFStore]:
    """trimmed list of available stores to be tested
    result should contain at least memory_rdf_store, and (if available) also include uri_rdf_store
    """
    stores = tuple(
        store
        for store in (_mem_rdf_store, _uri_rdf_store)
        if store is not None
    )
    return stores


def loadfilegraph(fname, format="json-ld"):
    graph = Graph()
    graph.parse(fname, format=format)
    return graph


@pytest.fixture()
def sample_file_graph():
    """graph loaded from specific input file
    in casu: tests/input/3293.jsonld
    """
    return loadfilegraph(str(TEST_INPUT_FOLDER / "3293.jsonld"))


def make_sample_graph(items: Iterable) -> Graph:
    g = Graph()
    for n in items:
        triple = tuple(
            URIRef(f"https://example.org/{part}#{n}")
            for part in ["subject", "predicate", "object"]
        )
        g.add(triple)
    return g


@pytest.fixture()
def example_graphs():
    return [make_sample_graph([i]) for i in range(10)]
