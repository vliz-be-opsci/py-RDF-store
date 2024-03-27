import os
import requests

import pytest
from rdflib import Graph, URIRef
from typing import Iterable
from pathlib import Path

from pyrdfstore import RDFStore, create_rdf_store
from util4tests import log, enable_test_logging


TEST_INPUT_FOLDER = Path(__file__).parent / "./input"


enable_test_logging()  # note that this includes loading .env into os.getenv


@pytest.fixture(scope="session")
def quicktest() -> bool:
    """ bool setting indicating to skip lengthy tests 
    setting driven by setting env variable "QUICKTEST" to anything but 0 or ""
    """
    return bool(os.getenv("QUICKTEST", 0))


@pytest.fixture(scope="session")
def _mem_rdf_store() -> RDFStore:
    """in memory store
    """
    log.debug("creating in memory rdf store")
    return create_rdf_store()


@pytest.fixture(scope="session")
def _uri_rdf_store() -> RDFStore:
    """proxy to available graphdb store
    But only if environment variables are set and service is available
    else None (which will result in trimming it from rdf_stores fixture)
    """
    write_uri = os.getenv("TEST_SPARQL_WRITE_URI", None)
    read_uri = os.getenv("TEST_SPARQL_READ_URI", write_uri)  # fall back to this
    # if no URI (or not accessible) provided - skip this by returning None
    if read_uri is None and write_uri is None:
        log.debug("not creating uri rdf store in test - no uri provided")
        return None
    for uri in (read_uri, write_uri):
        if not requests.get(uri).ok:
            log.debug(f"not creating uri rdf store in test - provided {uri=} not accesible")
            return None
    # else -- all is well
    log.debug(f"creating uri rdf store proxy to ({read_uri=}, {write_uri=})")
    return create_rdf_store(read_uri, write_uri)


@pytest.fixture()
def rdf_stores(_mem_rdf_store, _uri_rdf_store) -> Iterable[RDFStore]:
    """trimmed list of available stores to be tested
    """
    stores = tuple(store for store in (_mem_rdf_store, _uri_rdf_store) if store is not None)
    return stores


def loadfilegraph(fname, format="json-ld"):
    graph = Graph()
    graph.parse(fname, format=format)
    return graph


@pytest.fixture()
def sample_file_graph():
    """ graph loaded from specific input file
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
