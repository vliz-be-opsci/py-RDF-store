#! /usr/bin/env python
from util4tests import run_single_test
import pytest
import os
from string import ascii_lowercase
from pyrdfstore.store import RDFStoreAccess, RDFStore, URIRDFStore
import math
import random
from rdflib import Graph, URIRef
from pyrdfstore.common import QUERY_BUILDER


@pytest.mark.usefixtures("rdf_store")
def test_ingest(rdf_store):
    pass


@pytest.mark.usefixtures("prepopulated_rdf_store")
def test_select_subjects(prepopulated_rdf_store):
    assert (
        prepopulated_rdf_store is not None
    ), "can't perform test without target store"
    sparql = "SELECT ?subject ?p WHERE { ?subject ?p ?o }"
    tsa = RDFStoreAccess(prepopulated_rdf_store, QUERY_BUILDER)
    subjects = tsa.select_subjects(sparql)
    assert isinstance(subjects, list)
    assert len(subjects) > 0
    # Add more assertions as needed


def test_graph_to_batches():
    graph = Graph()
    groupsize = 2
    max_line = 4096 / groupsize
    stuffing = "<> <> <> . \n"
    available_len = max_line - len(stuffing)
    # Add a million triples to the graph
    cnt = 0
    for i in range(10):
        for c in ascii_lowercase:
            j = int(math.floor(random.randint(0, available_len) / 3))
            element_lengths = [i, j, (int(available_len) - (int(i) + int(j)))]
            # Create the list
            elements = [URIRef(c * int(element_lengths[k])) for k in range(3)]
            graph.add((elements[0], elements[1], elements[2]))
            cnt += 1

    print(f"{len(graph)=}")
    assert (
        len(graph) == cnt
    ), "we should have not created duplicate or missing triples"
    batches = URIRDFStore._graph_to_batches(graph)
    assert len(batches) > 0
    # total number of batches should be 260
    assert (
        len(batches) == cnt / groupsize
    ), f"the amount of batches should be count of all triples over groupsize"
    found_sizes = {len(grp.split("\n")) for grp in batches}
    expected_sizes = {groupsize}

    first_batch = batches[0].split("\n")
    print(f"{first_batch=}")
    # print(f"First batch split by newline: {batches[0].split('\n')}")
    assert (
        found_sizes == expected_sizes
    ), f"all batches should be of size {expected_sizes} not {found_sizes}"


@pytest.mark.usefixtures("rdf_store")
def test_insert(rdf_store):
    assert rdf_store is not None, "can't perform test without target store"

    # Create a test graph
    graph = Graph()
    graph.add(
        (
            URIRef("http://example.org/subject1"),
            URIRef("http://example.org/predicate1"),
            URIRef("http://example.org/object1"),
        )
    )
    graph.add(
        (
            URIRef("http://example.org/subject2"),
            URIRef("http://example.org/predicate2"),
            URIRef("http://example.org/object2"),
        )
    )

    # Call the insert method
    rdf_store.insert(graph)

    # Verify that the triples are inserted correctly
    sparql = "SELECT ?subject ?predicate ?object WHERE { ?subject ?predicate ?object }"
    tsa = RDFStoreAccess(rdf_store, QUERY_BUILDER)
    results = tsa.select_subjects(sparql)
    assert len(results) == 2
    assert (
        URIRef("http://example.org/subject1"),
        URIRef("http://example.org/predicate1"),
        URIRef("http://example.org/object1"),
    ) in results
    assert (
        URIRef("http://example.org/subject2"),
        URIRef("http://example.org/predicate2"),
        URIRef("http://example.org/object2"),
    ) in results


if __name__ == "__main__":
    run_single_test(__file__)
