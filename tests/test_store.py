#! /usr/bin/env python
from util4tests import run_single_test
import pytest
from typing import List, Tuple
from rdflib import Graph
from pyrdfstore.store import RDFStore
from logging import getLogger


log = getLogger("tests")


@pytest.mark.usefixtures("rdf_store", "example_graphs")
def test_fixtures(rdf_store: RDFStore, example_graphs: List[Graph]):
    assert rdf_store is not None, "fixture rdf-store should be available."
    assert (
        example_graphs is not None
    ), "fixture example_graphs should be available"
    assert (len(example_graphs) == 10)


@pytest.mark.usefixtures("rdf_store", "example_graphs")
def test_insert(rdf_store, example_graphs):
    assert rdf_store is not None, "can't perform test without target store"

    # Call the insert method
    num_triples = 2
    for i in range(num_triples):
        log.debug(f"attempt to insert simple graph #{i} --> \n  {example_graphs[i].serialize(format='turtle')=}\n<--\n")
        rdf_store.insert(example_graphs[i])

    # Verify that the triples are inserted correctly
    sparql = "SELECT * WHERE { ?s ?p ?o . }"
    # reformat SPARQLResult as List of Tuple of uri-str
    log.debug(f"trying out select {sparql=}")
    results: List[Tuple[str]] = [
        tuple(str(u) for u in r) for r in rdf_store.select(sparql)
    ]

    assert len(results) >= num_triples
    log.debug(f"{results=}")
    for i in range(num_triples):
        assert (
            tuple(
                f"https://example.org/{part}#{i}"
                for part in ["subject", "predicate", "object"]
            )
            in results
        )

# todo we need more tests to verify 
#   - the update-management
#   - the drops
#   ...

@pytest.mark.usefixtures("rdf_store", "example_graphs")
def test_insert_large(rdf_store, example_graphs):
    assert rdf_store is not None, "can't perform test without target store"

    # Read large file
    lg = Graph().parse("./tests/input/AffiliationInfo.ttl", format='turtle')
    num_triples = len(lg)
    log.debug(f"{num_triples}")

    # Call the insert method
    rdf_store.insert(lg, "<urn:test:large-turtle-test>")

    # Verify that the triples are parsed correctly
    sparql = "SELECT ?subject ?predicate ?object FROM <urn:test:large-turtle-test> WHERE { ?subject ?predicate ?object }"
    results: List[Tuple[str]] = [tuple(str(u) for u in r) for r in rdf_store.select(sparql)]

    assert len(results) == num_triples


@pytest.mark.usefixtures("rdf_store", "example_graphs")
def test_insert_large_statement(rdf_store, example_graphs):
    assert rdf_store is not None, "can't perform test without target store"

    # Read large statement 
    g = Graph().parse("./tests/input/marineinfo-publication-246614.ttl", format='turtle')
    pub_abstr = "".join([str(part) for part in g.objects(predicate=URIRef("http://purl.org/dc/terms/#abstract"))])
    log.debug(f"{pub_abstr=}")

    # Call the insert method
    rdf_store.insert(g, "<urn:test:publ246614>")
    
    # Verify that the large statement is parsed correctly
    sparql = "SELECT ?abstract WHERE { [] <http://purl.org/dc/terms/#abstract> ?abstract }"
    results: List[Tuple[str]] = [tuple(str(u) for u in r) for r in rdf_store.select(sparql)]
    log.debug(f"{sparql=}")
    log.debug(f"{results=}")
    assert results[-1][0] == pub_abstr


if __name__ == "__main__":
    run_single_test(__file__)
