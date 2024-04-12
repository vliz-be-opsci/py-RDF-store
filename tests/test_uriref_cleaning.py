#! /usr/bin/env python
from typing import Iterable

import pytest
from rdflib import BNode, Graph, Literal, URIRef
from util4tests import run_single_test

from pyrdfstore.store import (
    RDFStore,
    RDFStoreDecorator,
    check_valid_uri,
    clean_graph,
    clean_node,
    clean_uri,
)


def test_check_valid_uri():
    # Valid URIs
    assert check_valid_uri("https://example.com") is True
    assert check_valid_uri("http://example.com") is True
    assert check_valid_uri("ftp://example.com") is True
    assert check_valid_uri("file:///path/to/file") is False

    # Invalid URIs
    assert check_valid_uri("example.com") is False
    assert check_valid_uri("ftp://") is False
    assert check_valid_uri("file://") is False
    assert check_valid_uri("://example.com") is False


def test_clean_uri():
    # Test with safe_characters as None
    uri = "https://example.com"
    expected_result = "https://example.com"
    assert clean_uri(uri, None) == expected_result

    # Test with safe_characters as empty string
    uri = "https://example.com"
    expected_result = "https://example.com"
    assert clean_uri(uri, "") == expected_result

    # Test with safe_characters containing special characters
    uri = "https://example.com"
    safe_characters = ""
    expected_result = "https://example.com"
    assert clean_uri(uri, safe_characters) == expected_result

    # Test with safe_characters containing duplicate characters
    uri = "https://example.com"
    safe_characters = "~~@#$&()*!+=:;,?/'"
    expected_result = "https://example.com"
    assert clean_uri(uri, safe_characters) == expected_result

    # Test with safe_characters containing unique characters
    uri = "http://dx.doi.org/10.1656/1092-6194(2004)11[261:CBIAHC]2.0.CO;2"
    safe_characters = ""
    expected_result = (
        "http://dx.doi.org/10.1656/1092-6194(2004)11%5B261:CBIAHC%5D2.0.CO;2"
    )
    assert clean_uri(uri, safe_characters) == expected_result

    uri = "http://dx.doi.org/10.1656/1092-6194(2004)11[261:CBIAHC]2.0.CO;2"
    safe_characters = "[]"
    expected_result = (
        "http://dx.doi.org/10.1656/1092-6194(2004)11[261:CBIAHC]2.0.CO;2"
    )
    assert clean_uri(uri, safe_characters) == expected_result


def test_clean_node():
    # Test with non-URIRef input
    ref = BNode()
    safe_characters = ""
    assert clean_node(ref, safe_characters) == ref

    # Test with valid URIRef input
    ref = URIRef("https://example.com")
    safe_characters = ""
    assert clean_node(ref, safe_characters) == ref

    # Test with invalid URIRef input
    uri = "example.com"
    ref = URIRef(uri)
    safe_characters = ""
    expected_result = URIRef(clean_uri(uri, safe_characters))
    assert clean_node(ref, safe_characters) == expected_result

    # Test with valid URIRef input and safe_characters
    ref = URIRef("https://example.com")
    safe_characters = "~~@#$&()*!+=:;,?/'"
    assert clean_node(ref, safe_characters) == ref

    # Test with invalid URIRef input and safe_characters
    uri = "http://dx.doi.org/10.1656/1092-6194(2004)11[261:CBIAHC]2.0.CO;2"
    ref = URIRef(uri)
    safe_characters = ""
    expected_result = URIRef(clean_uri(uri, safe_characters))
    assert clean_node(ref, safe_characters) == expected_result

    uri = "http://dx.doi.org/10.1656/1092-6194(2004)11[261:CBIAHC]2.0.CO;2"
    ref = URIRef(uri)
    safe_characters = "[]"
    assert clean_node(ref, safe_characters) == ref


def test_clean_graph():
    # Test with an empty graph
    bgraph = Graph()
    safe_characters = ""
    expected_result = Graph()
    assert set(clean_graph(bgraph, safe_characters)) == set(expected_result)

    # Test with a graph containing a single triple
    bgraph = Graph()
    bgraph.add(
        (
            URIRef("https://example.com"),
            URIRef("https://example.com/predicate"),
            Literal("value"),
        )
    )
    safe_characters = ""
    expected_result = Graph()
    expected_result.add(
        (
            URIRef("https://example.com"),
            URIRef("https://example.com/predicate"),
            Literal("value"),
        )
    )
    assert set(clean_graph(bgraph, safe_characters)) == set(expected_result)

    # Test with a graph containing multiple triples
    bgraph = Graph()
    bgraph.add(
        (
            URIRef("https://example.com"),
            URIRef("https://example.com/predicate1"),
            Literal("value1"),
        )
    )
    bgraph.add(
        (
            URIRef("https://example.com"),
            URIRef("https://example.com/predicate2"),
            Literal("value2"),
        )
    )
    bgraph.add(
        (
            URIRef("https://example.com"),
            URIRef("https://example.com/predicate3"),
            Literal("value3"),
        )
    )
    safe_characters = ""
    expected_result = Graph()
    expected_result.add(
        (
            URIRef("https://example.com"),
            URIRef("https://example.com/predicate1"),
            Literal("value1"),
        )
    )
    expected_result.add(
        (
            URIRef("https://example.com"),
            URIRef("https://example.com/predicate2"),
            Literal("value2"),
        )
    )
    expected_result.add(
        (
            URIRef("https://example.com"),
            URIRef("https://example.com/predicate3"),
            Literal("value3"),
        )
    )
    assert set(clean_graph(bgraph, safe_characters)) == set(expected_result)

    # Test with a graph containing BNodes
    bgraph = Graph()
    bnode = BNode()
    bgraph.add(
        (
            URIRef("https://example.com"),
            URIRef("https://example.com/predicate"),
            bnode,
        )
    )
    safe_characters = ""
    expected_result = Graph()
    expected_result.add(
        (
            URIRef("https://example.com"),
            URIRef("https://example.com/predicate"),
            bnode,
        )
    )
    assert set(clean_graph(bgraph, safe_characters)) == set(expected_result)

    # Test with a graph containing URIs that need cleaning
    bgraph = Graph()
    bgraph.add(
        (
            URIRef("https://example.com/<>"),
            URIRef("https://example.com/predicate"),
            Literal("value"),
        )
    )
    safe_characters = "<>"
    expected_result = Graph()
    expected_result.add(
        (
            URIRef("https://example.com/<>"),
            URIRef("https://example.com/predicate"),
            Literal("value"),
        )
    )
    assert set(clean_graph(bgraph, safe_characters)) == set(expected_result)


@pytest.mark.usefixtures("rdf_stores")
def test_decorator_insert(rdf_stores: Iterable[RDFStore]):
    for rdf_store in rdf_stores:
        decorator = RDFStoreDecorator(rdf_store)
        graph = Graph()
        graph.add(
            (
                URIRef("https://example.com/example"),
                URIRef("https://example.com/[]predicate"),
                Literal("value"),
            )
        )
        decorator.insert(graph)
        result = decorator.select(
            "SELECT ?p WHERE { <https://example.com/example> ?p ?o }"
        )
        for row in result:
            assert row[0] == URIRef("https://example.com/%5B%5Dpredicate")


if __name__ == "__main__":
    run_single_test(__file__)
