#! /usr/bin/env python
from util4tests import run_single_test
import pytest
from string import ascii_lowercase
from typing import List, Tuple
import math
import random
from rdflib import Graph, URIRef
from pyrdfstore.store import RDFStore, URIRDFStore
from logging import getLogger


log = getLogger("tests")


@pytest.mark.usefixtures("rdf_store", "example_graphs")
def test_fixtures(rdf_store: RDFStore, example_graphs: List[Graph]):
    assert rdf_store is not None, "fixture rdf-store should be available."
    assert (
        example_graphs is not None
    ), "fixture example_graphs should be available"
    assert (len(example_graphs) == 10)


def test_graph_to_batches():
    graph = Graph()
    # we want to chunk-up to result in equal sized groups by having each line this short
    groupsize = 2
    max_line = math.floor(4096 / groupsize)
    stuffing = "<> <> <> . \n"   # the overhead chars that will be added
    available_len = max_line - len(stuffing)
    # Add a bunch of triples to the graph
    cnt = 0
    for i in range(10):
        for c in ascii_lowercase:
            j = math.floor(random.randint(0, available_len) / 3)
            uri_lengths = [i, j, available_len - (i + j)]
            # Create the list
            triple = tuple(URIRef(c * int(uri_lengths[k])) for k in range(3))
            graph.add(triple)
            cnt += 1

    log.debug(f"{len(graph)=}")
    assert (
        len(graph) == cnt
    ), "we should have not created duplicate or missing triples"
    batches = URIRDFStore._graph_to_batches(graph)

    # total number of batches should be 260
    assert len(batches) > 0
    assert (
        len(batches) == cnt / groupsize
    ), "the amount of batches should be count of all triples over groupsize"
    found_sizes = {len(grp.split("\n")) for grp in batches}
    expected_sizes = {groupsize}

    first_batch = batches[0].split("\n")
    log.debug(f"{first_batch=}")
    # print(f"First batch split by newline: {batches[0].split('\n')}")
    assert (
        found_sizes == expected_sizes
    ), f"all batches should be of size {expected_sizes} not {found_sizes}"


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


if __name__ == "__main__":
    run_single_test(__file__)
