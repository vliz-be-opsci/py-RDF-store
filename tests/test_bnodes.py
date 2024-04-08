#! /usr/bin/env python
from typing import Iterable
from uuid import uuid4

import pytest
from conftest import (
    DCT_ABSTRACT,
    TEST_INPUT_FOLDER,
    assert_file_ingest,
    loadfilegraph,
    make_sample_graph,
)
from rdflib import BNode, Graph, Literal
from util4tests import log, run_single_test

from pyrdfstore.store import RDFStore


@pytest.mark.usefixtures("rdf_stores")
def test_insert_simple_with_bnodes(rdf_stores: Iterable[RDFStore]):
    for rdf_store in rdf_stores:
        # check the ingest of a simple example using blank nodes
        assert_file_ingest(
            rdf_store, TEST_INPUT_FOLDER / "simple_with_bnodes.ttl"
        )


@pytest.mark.usefixtures("rdf_stores")
def test_file_with_blanknodes(rdf_stores: Iterable[RDFStore]):
    """specific test for issue #32
    making sure distinct blanknodes are indeed considered separate after ingest
    """
    g: Graph = loadfilegraph(
        TEST_INPUT_FOLDER / "issue-32.ttl", format="turtle"
    )
    num_things_in_file = 4
    ns: str = f"urn:test:uuid:{uuid4()}"
    sparql: str = (
        "prefix schema: <https://schema.org/>"
        "select distinct ?s "
        "where { ?s a schema:Thing .}"
    )

    for rdf_store in rdf_stores:
        rdf_store_type = type(rdf_store).__name__
        rdf_store.insert(g, ns)
        result = rdf_store.select(sparql, ns)
        assert len(result) == num_things_in_file, (
            f"{rdf_store_type} :: "
            f"issue/32 unexpected response length {len(result)=} "
            f"not {num_things_in_file=}"
        )
        log.debug(
            f"{rdf_store_type} :: no issue/32 detected {sparql=} "
            f"and got {len(result)=}"
        )


@pytest.mark.usefixtures("rdf_stores")
def test_insert_with_skolemize(rdf_stores: Iterable[RDFStore]):
    # Create a test graph with BNODE
    graph = Graph()
    graph.add((BNode(), DCT_ABSTRACT, Literal("Test abstract of blank node")))

    sparql = f"SELECT ?abstract WHERE {{ [] <{DCT_ABSTRACT}> ?abstract }}"

    for rdf_store in rdf_stores:
        rdf_store_type = type(rdf_store).__name__

        result_before = rdf_store.select(sparql)
        log.debug(f"{result_before=}")
        log.debug(f"{len(result_before)=}")

        # Call the insert method
        rdf_store.insert(graph)

        result = rdf_store.select(sparql)
        log.debug(f"{rdf_store_type} :: {result=}")
        log.debug(f"{rdf_store_type} :: {len(result)=}")
        n = 0
        for row in result:
            log.debug(f"{rdf_store_type} :: {n=} : {row=}")
            n += 1

        assert len(result) == len(result_before) + 1, (
            f"{rdf_store_type} :: " "we should have added one abstract !"
        )


@pytest.mark.usefixtures("rdf_stores")
def test_separate_blanknodes(rdf_stores: Iterable[RDFStore]):
    """specific test for issue #32
    making sure distinct blanknodes are indeed considered separate after ingest
    """
    lbl: str = "issue-32"
    base: str = f"https://example.org/base-{lbl}/"
    num: int = 5
    start: int = 200
    g: Graph = make_sample_graph(
        range(start, start + num), base=base, bnode_subjects=True
    )
    ns: str = f"urn:test-{lbl}:uuid:{uuid4()}"
    sparql: str = "select distinct ?s where { ?s ?p ?o .}"

    for rdf_store in rdf_stores:
        rdf_store_type = type(rdf_store).__name__
        rdf_store.insert(g, ns)
        result = rdf_store.select(sparql, ns)
        assert len(result) == num, (
            f"{rdf_store_type} :: "
            f"issue/32 unexpected response length {len(result)=} not {num=}"
        )
        log.debug(
            f"{rdf_store_type} :: no issue/32 detected {sparql=} "
            f" and got {len(result)=}"
        )


@pytest.mark.usefixtures("rdf_stores")
def test_separate_blanknodes_in_distinct_graphs(
    rdf_stores: Iterable[RDFStore],
):
    """specific test for issue #42
    making sure distinct blanknodes are indeed considered separate after ingest
    even if that ingest involves multiple graphs
    """
    lbl: str = "issue-42"
    base: str = f"https://example.org/base-{lbl}/"
    N: int = 3  # we will make 3 graphs with distinct bnodes
    num: int = 5  # each graph will have 5 bnode subjects
    start: int = 420
    graphs: Iterable[Graph] = tuple(
        make_sample_graph(
            range(start + n * num, start + (n + 1) * num),
            base=base,
            bnode_subjects=True,
        )
        for n in range(N)
    )
    ns: str = f"urn:test-{lbl}:uuid:{uuid4()}"
    sparql: str = "select distinct ?s where { ?s ?p ?o .}"

    for rdf_store in rdf_stores:
        rdf_store_type = type(rdf_store).__name__
        log.debug(
            f"{rdf_store_type} :: testing for issue/42 "
            f"inserting {len(graphs)} graphs of {num} bnode-subjects "
            f"into named_graph {ns}"
        )
        for i, g in enumerate(graphs):
            log.debug(
                f"{rdf_store_type} :: insert graph#{i:02d} "
                f"of size {len(g)} into {ns}"
            )
            rdf_store.insert(g, ns)
        result = rdf_store.select(sparql, ns)
        assert len(result) != num, (
            f"{rdf_store_type} :: "
            f"issue/42 unexpected response length {len(result)=} not {num=} "
            "this shows the various graphs inserted all got the same uri"
        )
        assert len(result) == N * num, (
            f"{rdf_store_type} :: "
            f"issue/42 unexpected response length {len(result)=} not {num=} "
            "this shows we can't even predict how many / which unique uri "
            "are used for bnodes during skolemnization to {rdf_store_type}"
        )
        log.debug(
            f"{rdf_store_type} :: no issue/42 detected {sparql=} "
            f" and got {len(result)=}"
        )


if __name__ == "__main__":
    run_single_test(__file__)
