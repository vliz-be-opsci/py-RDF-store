#! /usr/bin/env python
from util4tests import run_single_test
import pytest
from typing import List, Tuple
from rdflib import Graph, Namespace, URIRef
from pyrdfstore.store import RDFStore
from logging import getLogger
from time import sleep


log = getLogger("tests")
DCT: Namespace = Namespace("http://purl.org/dc/terms/#")
DCT_ABSTRACT: URIRef = DCT.abstract
SELECT_ALL_SPO = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . }"


@pytest.mark.usefixtures("rdf_store", "example_graphs")
def test_fixtures(rdf_store: RDFStore, example_graphs: List[Graph]):
    assert rdf_store is not None, "fixture rdf-store should be available."
    assert (
        example_graphs is not None
    ), "fixture example_graphs should be available"
    assert (len(example_graphs) == 10)


@pytest.mark.usefixtures("rdf_store", "example_graphs")
def test_insert(rdf_store: RDFStore, example_graphs: List[Graph]):

    # Call the insert method
    nums = range(2)
    for i in nums:
        log.debug(f"attempt to insert simple graph #{i} --> \n  {example_graphs[i].serialize(format='turtle')=}\n<--\n")
        rdf_store.insert(example_graphs[i])

    # Verify that the triples are inserted correctly
    sparql = SELECT_ALL_SPO
    # reformat SPARQLResult as List of Tuple of uri-str
    log.debug(f"trying out select {sparql=}")
    results: List[Tuple[str]] = [
        tuple(str(u) for u in r) for r in rdf_store.select(sparql)
    ]

    assert len(results) >= len(nums)
    log.debug(f"{results=}")
    for i in nums:
        assert (
            tuple(
                f"https://example.org/{part}#{i}"
                for part in ["subject", "predicate", "object"]
            )
            in results
        ), f"execpted triple for index { i } not found in search result."


@pytest.mark.usefixtures("rdf_store")
def test_insert_large(rdf_store: RDFStore):
    assert rdf_store is not None, "can't perform test without target store"

    ns = "urn:test:large-turtle-test"

    # Read large file
    lg = Graph().parse("./tests/input/AffiliationInfo.ttl", format='turtle')
    num_triples = len(lg)
    log.debug(f"{num_triples}")

    # Call the insert method
    rdf_store.insert(lg, ns)

    # Verify that the triples are parsed correctly
    sparql = SELECT_ALL_SPO
    results: List[Tuple[str]] = [tuple(str(u) for u in r) for r in rdf_store.select(sparql, ns)]

    assert len(results) == num_triples


@pytest.mark.usefixtures("rdf_store")
def test_insert_large_statement(rdf_store: RDFStore):
    assert rdf_store is not None, "can't perform test without target store"

    ns = "urn:test:publication-246614"

    # Read large statement
    g = Graph().parse("./tests/input/marineinfo-publication-246614.ttl", format='turtle')
    pub_abstr = "".join([str(part) for part in g.objects(predicate=DCT_ABSTRACT)])
    log.debug(f"{pub_abstr=}")

    # Call the insert method
    rdf_store.insert(g, ns)

    # Verify that the large statement is parsed correctly
    sparql = f"SELECT ?abstract WHERE {{ [] <{ DCT_ABSTRACT }> ?abstract }}"
    results: List[Tuple[str]] = [tuple(str(u) for u in r) for r in rdf_store.select(sparql, ns)]
    log.debug(f"{sparql=}")
    log.debug(f"{results=}")
    assert results[-1][0] == pub_abstr


@pytest.mark.usefixtures("rdf_store", "example_graphs")
def test_insert_named(rdf_store: RDFStore, example_graphs: List[Graph]):

    # we will create 2 named_graphs, and have them contain some overlapped ranges from the example_graphs
    plans = [
        dict(ns=f"urn:test-space:{ i }", nums=range(3*i, 4*i+1))
        for i in range(2)
    ]
    # we always look for all triples in the graph
    sparql = SELECT_ALL_SPO

    # insert the selected range per ns
    for plan in plans:
        ns = plan['ns']
        nums = plan['nums']
        g = Graph()
        for num in nums:
            g += example_graphs[num]
        rdf_store.insert(g, ns)

        # reformat SPARQLResult as List of Tuple of uri-str
        results: List[Tuple[str]] = [
            tuple(str(u) for u in r) for r in rdf_store.select(sparql)
        ]

        log.debug(f"found in {ns=} --> {len(results)=}")
        assert len(results) >= len(nums)

        for i in nums:
            assert (
                tuple(
                    f"https://example.org/{part}#{i}"
                    for part in ["subject", "predicate", "object"]
                )
                in results
            ), f"expected triple for index { i } not found in result"

    for plan in plans:
        assert rdf_store.verify_max_age(ns, 1), "graphs should be inserted and checked in less then a minute"

    sleep(60)  # hey, seriuosly? wait a minute!
    for plan in plans:
        assert not rdf_store.verify_max_age(ns, 1), "after a minute of nothing, those should be older then a minute"

    # now drop the second graph, and check for the (should be none!) effect on the first
    ns1, nums1 = plans[0]['ns'], plans[0]['nums']
    ns2 = plans[1]['ns']
    rdf_store.drop_graph(ns2)
    assert rdf_store.verify_max_age(ns2, 1), "dropped graph should be marked as changed again"

    # there should be nothing left in ns2
    try:
        result = rdf_store.select(sparql, ns2)
    except Exception:  # accept that this could also throw an exception since the graph was dropped!
        result = []
    assert len(result) == 0, "there should be no results in a dropped ns"

    # there should be nothing changed in ns1
    result: List[Tuple[str]] = [
        tuple(str(u) for u in r) for r in rdf_store.select(sparql, ns1)
    ]
    assert len(result) == len(nums1), "there should still be same results in the kept ns"
    for i in nums1:
        assert (
            tuple(
                f"https://example.org/{part}#{i}"
                for part in ["subject", "predicate", "object"]
            )
            in results
        ), f"expected triple for index { i } not found in result of {ns1=}"

    # all stuff should still be there in the overall search-graph
    result: List[Tuple[str]] = [
        tuple(str(u) for u in r) for r in rdf_store.select(sparql)
    ]
    assert len(result) >= len(nums1), "there should be at least same results in the overall store"
    for i in nums1:
        assert (
            tuple(
                f"https://example.org/{part}#{i}"
                for part in ["subject", "predicate", "object"]
            )
            in results
        ), f"expected triple for index { i } not found in result of overall search"


if __name__ == "__main__":
    run_single_test(__file__)
