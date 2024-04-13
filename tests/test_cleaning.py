#! /usr/bin/env python
from conftest import TEST_INPUT_FOLDER, format_from_extension, loadfilegraph
from rdflib import BNode, Graph, URIRef, Literal, Namespace
from util4tests import log, run_single_test
from typing import Callable
import json

from pyrdfstore.clean import (
    reparse,
    check_valid_uri, clean_uri_node, clean_uri_str,
    normalise_scheme_str, normalise_scheme_node,
    Level, NAMED_CLEAN_FUNCTIONS,
    build_clean_chain, clean_graph,
)

SCHEMA: Namespace = Namespace("https://schema.org/")
RDF: Namespace = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
EX: Namespace = Namespace("http://example.org/")


def test_compare_bnodes_ttl_jsonld():
    localnames = ["b0", "b1", "x9"]
    tst_content = dict()

    ttl = "\n".join(f"_:{ln} a <{EX}MyType> ." for ln in localnames)
    tst_content["turtle"] = ttl

    js = ",\n  ".join(
        f'{{"@id": "_:{ln}", "@type": "{EX.MyType}" }}' for ln in localnames
    )
    tst_content["json-ld"] = f"[\n  {js} ]"

    for fmt, cntnt in tst_content.items():
        log.debug(f"{fmt=}, cntnt ->\n{cntnt}\n<-")
        g = reparse(Graph().parse(data=cntnt, format=fmt))

        items = list(g.subjects(predicate=RDF.type, object=EX.MyType))
        assert len(items) == 3
        for i, item in enumerate(items):
            log.debug(f". found {i} -> {type(item).__name__} {item.n3()=} ")
            assert isinstance(item, BNode)
            assert item.n3()[2:] not in localnames, (
                f"problem with format {fmt}"
            )


def test_compare_bnodes_ttl_jsonld_files():
    """testing how rdlib jsonld parser is handling the "@id": "_:n1" in json
    see bug in rdflib: https://github.com/RDFLib/rdflib/issues/2760
    """
    for fname in ("issue-42.jsonld", "issue-42.ttl"):
        localnames = [
            "b0",
            "b1",
            "x9",
        ]  # known local id to be used in these files
        log.debug(f"testing ${fname=}")
        fpath = TEST_INPUT_FOLDER / fname

        g: Graph = reparse(
            loadfilegraph(fpath, format=format_from_extension(fpath))
        )
        persons = g.subjects(predicate=RDF.type, object=SCHEMA.Person)
        for i, p in enumerate(persons):
            log.debug(f". found {i} -> {type(p).__name__} {p.n3()=} ")
            assert isinstance(p, BNode)
            assert p.n3()[2:] not in localnames, (
                "local id {p.n3()} used in file should have been replaced"
            )


def test_clean_uri_str():
    bad_uri: str = "https://example.org/with-[square]-brackets"
    assert not check_valid_uri(bad_uri)

    force_safe = clean_uri_str(bad_uri)
    force_safe_again = clean_uri_str(force_safe)

    smart_safe = clean_uri_str(bad_uri, smart=True)
    smart_safe_again = clean_uri_str(smart_safe, smart=True)

    assert force_safe == smart_safe
    assert force_safe != force_safe_again, (
        "forced cleaning should not be idempotent"
    )
    assert smart_safe == smart_safe_again, (
        "smart cleaning should be idempotent"
    )


def test_clean_uri_node():
    bad_uri: str = "https://example.org/with-[square]-brackets"
    bad_ref_node: URIRef = URIRef(bad_uri)
    blank_node: BNode = BNode()
    literal_node: Literal = Literal(bad_uri, datatype="xsd:string")

    good_ref_node = clean_uri_node(bad_ref_node)
    assert bad_ref_node != good_ref_node
    assert not check_valid_uri(str(bad_ref_node))
    assert check_valid_uri(str(good_ref_node))

    assert blank_node == clean_uri_node(blank_node)
    assert literal_node == clean_uri_node(literal_node)


def test_normalise_scheme_node():
    http_uri = "http://schema.org/test"
    https_uri = "https://schema.org/test"

    assert https_uri == normalise_scheme_str(http_uri)
    assert https_uri == normalise_scheme_str(https_uri)
    assert https_uri == str(normalise_scheme_node(URIRef(http_uri)))
    assert https_uri == str(normalise_scheme_node(URIRef(https_uri)))

    domain = "example.org"
    http_domain = f"http://{domain}/tester"
    https_domain = f"https://{domain}/tester"

    # in this domain we want to force http scheme
    assert http_domain == normalise_scheme_str(http_domain, domain=domain, to_scheme="http")
    assert http_domain == normalise_scheme_str(https_domain, domain=domain, to_scheme="http")
    assert http_domain == str(normalise_scheme_node(URIRef(http_domain), domain=domain, to_scheme="http"))
    assert http_domain == str(normalise_scheme_node(URIRef(https_domain), domain=domain, to_scheme="http"))

    no_domain = "none.ext"
    http_no_domain = f"http://{no_domain}/ignored"
    https_no_domain = f"https://{no_domain}/ignored"

    # and that should ignore uri from other domains
    assert http_no_domain == normalise_scheme_str(http_no_domain, domain=domain, to_scheme="http")
    assert https_no_domain == normalise_scheme_str(https_no_domain, domain=domain, to_scheme="http")
    assert http_no_domain == str(normalise_scheme_node(URIRef(http_no_domain), domain=domain, to_scheme="http"))
    assert https_no_domain == str(normalise_scheme_node(URIRef(https_no_domain), domain=domain, to_scheme="http"))


def test_clean_chain():
    count_triples: int = 0

    def custom_triple_filter(t: tuple) -> tuple:
        nonlocal count_triples
        count_triples += 1  # testable side-effect
        return t  # do no real filtering
    custom_triple_filter.level = Level.Triple

    specs = list(NAMED_CLEAN_FUNCTIONS.keys())  # apply all filters
    specs.append(custom_triple_filter)  # and our own

    cleaner: Callable = build_clean_chain(*specs)
    graph: Graph = Graph()  # the testgraph to clean

    bnode_name: str = "problematic_blanknode"
    json_data: str = json.dumps({
        "@id": f"_:{bnode_name}",
        "@type": EX.TestType
    })
    log.debug(f"parsing {json_data=}")
    graph.parse(data=json_data, format="json-ld")

    bad_schema_org_triple: tuple = tuple((
        BNode(),
        URIRef("http://schema.org/one"),
        Literal("schema.org-clean-test")
    ))
    graph.add(bad_schema_org_triple)

    bad_uri_triple = tuple((
        BNode(),
        SCHEMA.downloadUrl,
        URIRef("http://example.org/")
    ))
    graph.add(bad_uri_triple)

    log.debug(f"cleaning:\n{graph.serialize(format='nt')}")
    cleaned = clean_graph(graph, cleaner)
    log.debug(f"cleaned to:\n{cleaned.serialize(format='nt')}")

    # assert all issues vanished
    count_bnodes:int = 0
    count_literals: int = 0
    count_uriref: int = 0
    count_other: int = 0
    for t in cleaned.triples(tuple((None, None, None))):
        for n in t:
            if isinstance(n, Literal):
                count_literals += 1
                continue
            # else
            if isinstance(n, BNode):
                count_bnodes += 1
                assert str(n) != bnode_name
                continue
            # else
            if isinstance(n, URIRef):
                count_uriref += 1
                uri = str(n)
                assert check_valid_uri(uri)
                if ("schema.org") in uri:
                    assert uri.startswith("https://")
                continue
            # else
            count_other += 1

    # assert all triples where visited
    expected_bnodes: int = 3
    expected_literals: int = 1
    expected_uriref = len(graph) * 3 - expected_bnodes - expected_literals
    expected_other: int = 0

    assert count_triples == len(graph)
    assert expected_bnodes == count_bnodes
    assert expected_literals == count_literals
    assert expected_uriref == count_uriref
    assert expected_other == count_other




if __name__ == "__main__":
    run_single_test(__file__)