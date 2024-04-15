#! /usr/bin/env python
from time import sleep
from typing import Iterable, List, Tuple
from urllib.parse import quote
from uuid import uuid4

import pytest
from conftest import (
    DCT_ABSTRACT,
    SELECT_ALL_SPO,
    TEST_INPUT_FOLDER,
    assert_file_ingest,
)
from rdflib import Graph, Literal, URIRef
from rdflib.query import Result
from util4tests import log, run_single_test

from pyrdfstore.store import RDFStore


@pytest.mark.usefixtures("rdf_stores", "example_graphs")
def test_fixtures(rdf_stores: Iterable[RDFStore], example_graphs: List[Graph]):
    for rdf_store in rdf_stores:
        rdf_store_type = type(rdf_store).__name__
        assert rdf_store is not None, (
            f"{rdf_store_type} :: " "fixture rdf-store should be available."
        )
    assert (
        example_graphs is not None
    ), "fixture example_graphs should be available"
    assert len(example_graphs) == 10


@pytest.mark.usefixtures("rdf_stores")
def test_uri_with_odd_chars(rdf_stores: Iterable[RDFStore]):
    doi: str = (
        "http://dx.doi.org/10.1656/1092-6194(2004)11[261:CBIAHC]2.0.CO;2"
    )
    g: Graph = Graph().add(
        tuple((URIRef(doi), DCT_ABSTRACT, Literal("something something")))
    )

    # safe variants
    doi_sf = quote(doi, safe="~@#$&()*!+=:;,?/'")
    g_sf: Graph = Graph().add(
        tuple(
            (URIRef(doi_sf), DCT_ABSTRACT, Literal("something now made safe"))
        )
    )
    log.debug(f"{doi_sf=}")
    ns: str = f"urn:test-uri-with-strange-chars:{uuid4()}"
    for rdf_store in rdf_stores:
        rdf_store_type: str = type(rdf_store).__name__
        before: int = 0
        try:  # this can fail (but does not need to)
            rdf_store.insert(g, ns)
            log.debug(
                f"{rdf_store_type} :: inserting triple with funny chars "
                f"in URI to {ns=}"
            )
            res: Result = rdf_store.select(SELECT_ALL_SPO, ns)
            assert len(res) == len(g)
            before: int = len(res)
        except Exception:
            pass  # some stores (like MemRDFStore) do accept, which is ok
        rdf_store.insert(g_sf, ns)
        log.debug(
            f"{rdf_store_type} :: retry inserting triple with escaped chars "
            f"in URI to {ns=}"
        )
        res: Result = rdf_store.select(SELECT_ALL_SPO, ns)
        assert len(res) == len(g_sf) + before
        log.debug(
            f"{rdf_store_type} :: @end of funny char test with {len(res)=}"
        )


@pytest.mark.usefixtures("rdf_stores")
def test_unknown_graph_age(rdf_stores: Iterable[RDFStore]):
    """specific test for issue #47"""
    unknown_ng: str = f"urn:test:unkown-graph-age:{uuid4()}"
    for rdf_store in rdf_stores:
        rdf_store_type: str = type(rdf_store).__name__
        assert not rdf_store.verify_max_age(unknown_ng, 1), (
            f"{rdf_store_type} :: verification of max_age for unknown graphs "
            "should always return False, and not throw KeyError"
        )


@pytest.mark.usefixtures("rdf_stores", "example_graphs")
def test_insert(rdf_stores: Iterable[RDFStore], example_graphs: List[Graph]):
    nums = range(2)

    for rdf_store in rdf_stores:
        rdf_store_type = type(rdf_store).__name__
        # Call the insert method
        for i in nums:
            log.debug(
                f"""{rdf_store_type} :: attempt to insert simple graph #{i} -->
                {example_graphs[i].serialize(format='turtle')=}
                <-- """
            )
            rdf_store.insert(example_graphs[i])

        # Verify that the triples are inserted correctly
        sparql = SELECT_ALL_SPO
        # reformat SPARQLResult as List of Tuple of uri-str
        log.debug(f"{rdf_store_type} :: trying out select {sparql=}")
        spo_result = rdf_store.select(sparql)
        spo_result_type = type(spo_result).__name__
        log.debug(f"{rdf_store_type} :: {spo_result=} | {spo_result_type=}")
        assert isinstance(spo_result, Result), (
            f"{rdf_store_type} :: "
            f"{spo_result=} | {spo_result_type} is not a real Result"
        )
        results: List[Tuple[str]] = [
            tuple(str(u) for u in r) for r in spo_result
        ]

        assert len(results) >= len(nums)
        log.debug(f"{results=}")
        for i in nums:
            assert (
                tuple(
                    f"https://example.org/{part}-{i}"
                    for part in ["subject", "predicate", "object"]
                )
                in results
            ), (
                f"{rdf_store_type} :: expected triple for index { i } "
                "not found in search result."
            )


@pytest.mark.usefixtures("rdf_stores")
def test_unkown_drop(rdf_stores: Iterable[RDFStore]):
    ns = f"urn:test:uuid:{uuid4()}"
    log.debug(f"trying to drop non-existant {ns=}")

    for rdf_store in rdf_stores:
        rdf_store_type = type(rdf_store).__name__
        rdf_store.drop_graph(ns)
        # we should just get here without an error
        # and we should have a trace of its delete
        assert rdf_store.verify_max_age(ns, 1), (
            f"{rdf_store_type} :: "
            f"named_graph {ns=} latest change should be traceable"
        )

        log.debug(f"{rdf_store_type} :: {list(rdf_store.named_graphs)=}")
        assert ns in rdf_store.named_graphs, (
            f"{rdf_store_type} :: "
            f"named_graph {ns=} should be in list of known graphs"
        )

        # now remove the update-trace
        rdf_store.forget_graph(ns)
        assert ns not in rdf_store.named_graphs


@pytest.mark.usefixtures("rdf_stores")
def test_insert_large_graph(rdf_stores: Iterable[RDFStore]):
    for rdf_store in rdf_stores:
        # check the ingest of a large turtle file with many triuples
        assert_file_ingest(rdf_store, TEST_INPUT_FOLDER / "large_turtle.ttl")


@pytest.mark.usefixtures("rdf_stores")
def test_insert_large_statement(rdf_stores: Iterable[RDFStore]):
    sparql = f"SELECT ?abstract WHERE {{ [] <{ DCT_ABSTRACT }> ?abstract }}"

    for rdf_store in rdf_stores:
        rdf_store_type = type(rdf_store).__name__

        # check the ingest of a turtle file with
        # one really large value in the dct:abstract
        g, ns, result = assert_file_ingest(
            rdf_store,
            TEST_INPUT_FOLDER / "marineinfo-publication-246614.ttl",
            sparql,
            1,
        )

        # check the inserted large abstract from before insert
        pub_abstr = "".join(
            [str(part) for part in g.objects(predicate=DCT_ABSTRACT)]
        )
        log.debug(f"{rdf_store_type} :: {len(pub_abstr)=}")

        # Verify that the large content rountripped nicely
        result: List[Tuple[str]] = [tuple(str(u) for u in r) for r in result]
        assert result[-1][0] == pub_abstr, (
            f"{rdf_store_type} :: "
            f"mismatch {pub_abstr=} != '{result[-1][0]}'"
        )


@pytest.mark.usefixtures("rdf_stores", "example_graphs", "quicktest")
def test_insert_named(
    rdf_stores: Iterable[RDFStore],
    example_graphs: List[Graph],
    quicktest: bool,
):

    # this test plans to create 2 named_graphs,
    # so they contain some overlapped ranges from the example_graphs fixture
    plans = [
        dict(ns=f"urn:test-space:{ i }", nums=range(3 * i, 4 * i + 1))
        for i in range(2)
    ]
    # we always look for all triples in the graph
    sparql = SELECT_ALL_SPO

    for rdf_store in rdf_stores:
        rdf_store_type = type(rdf_store).__name__

        # insert the selected range per ns
        for plan in plans:
            ns = plan["ns"]
            nums = plan["nums"]
            g = Graph()
            for num in nums:
                g += example_graphs[num]
            rdf_store.insert(g, ns)

            # reformat SPARQLResult as List of Tuple of uri-str
            results: List[Tuple[str]] = [
                tuple(str(u) for u in r) for r in rdf_store.select(sparql)
            ]

            log.debug(f"{rdf_store_type} :: in {ns=} -> {len(results)=}")
            assert len(results) >= len(nums)

            for i in nums:
                assert (
                    tuple(
                        f"https://example.org/{part}-{i}"
                        for part in ["subject", "predicate", "object"]
                    )
                    in results
                ), (
                    f"{rdf_store_type} :: expected triple for index { i } "
                    "not found in result"
                )

        for plan in plans:
            assert rdf_store.verify_max_age(ns, 1), (
                f"{rdf_store_type} :: "
                "graphs should be inserted and checked in less then a minute"
            )

        if quicktest:
            # skip the remainder of the test involving wait times
            continue
        # else do the rest of the test which even involves taking wait time...

        log.info("into slow part of the test - skip by setting 'quicktest'")
        sleep(60)  # hey, seriuosly? wait a minute!
        for plan in plans:
            assert not rdf_store.verify_max_age(ns, 1), (
                f"{rdf_store_type} :: "
                "after a minute of nothing, "
                "all those graphs should be older then a minute"
            )

        # now drop the second graph,
        # and check for the (should be none!) effect on the first
        ns1, nums1 = plans[0]["ns"], plans[0]["nums"]
        ns2 = plans[1]["ns"]
        rdf_store.drop_graph(ns2)
        assert rdf_store.verify_max_age(ns2, 1), (
            f"{rdf_store_type} :: "
            "dropped graph should be marked as changed again"
        )

        # there should be nothing left in ns2
        try:
            result = rdf_store.select(sparql, ns2)
        except Exception:
            # note this could also throw an exception in case
            # the graph was dropped!
            result = []
        assert len(result) == 0, (
            f"{rdf_store_type} :: "
            "there should be no results in a dropped ns"
        )

        # there should be nothing changed in ns1
        result: List[Tuple[str]] = [
            tuple(str(u) for u in r) for r in rdf_store.select(sparql, ns1)
        ]
        assert len(result) == len(nums1), (
            f"{rdf_store_type} :: "
            "there should still be same results in the kept ns"
        )
        for i in nums1:
            assert (
                tuple(
                    f"https://example.org/{part}-{i}"
                    for part in ["subject", "predicate", "object"]
                )
                in results
            ), (
                f"{rdf_store_type} :: expected triple for index { i } "
                f"not found in result of {ns1=}"
            )

        # all stuff should still be there in the overall search-graph
        result: List[Tuple[str]] = [
            tuple(str(u) for u in r) for r in rdf_store.select(sparql)
        ]
        assert len(result) >= len(nums1), (
            f"{rdf_store_type} :: "
            "there should be at least same results in the overall store"
        )
        for i in nums1:
            assert (
                tuple(
                    f"https://example.org/{part}-{i}"
                    for part in ["subject", "predicate", "object"]
                )
                in results
            ), (
                f"{rdf_store_type} :: expected triple for index { i } "
                "not found in result of overall search"
            )


@pytest.mark.usefixtures("rdf_stores", "sample_file_graph")
def test_select_property_trajectory(
    rdf_stores: Iterable[RDFStore], sample_file_graph
):
    # SPARQL to select trajectory of a property
    sparql = """
    SELECT ?s ?o WHERE {
    ?s <http://purl.org/dc/terms/abstract>/<http://purl.org/dc/terms/else> ?o .
    }
    """

    for rdf_store in rdf_stores:
        rdf_store.insert(sample_file_graph)

        rdf_store.select(sparql)
        # we should just get here without an error
        assert True


if __name__ == "__main__":
    run_single_test(__file__)
