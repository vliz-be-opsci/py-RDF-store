#! /usr/bin/env python
from rdflib import Graph, URIRef
from pyrdfstore.store import URIRDFStore


TEST_SPARQL_READ_URI = "http://localhost:7200/repositories/rdf_store_test"
TEST_SPARQL_WRITE_URI = "http://localhost:7200/repositories/rdf_store_test/statements"
SPO = ["subj", "pred", "obj"]

ns = "urn:mpo-test:1"
ns2 = "urn:mpo-test:2"
us = URIRDFStore(TEST_SPARQL_READ_URI, TEST_SPARQL_WRITE_URI)


def checks(when: str, ns: str = None):
    r = us.select("select * where {?s ?p ?o .}")
    print(f"{when=} spo check {len(r)=}")
    lm = us.lastmod_for_named_graph(ns)
    print(f"{when=} lm check {lm=} type:({type(lm) if lm is not None else 'None'})")


def main():
    checks("initial")

    us.drop_graph(ns)
    checks("after-drop")

    ig = Graph()
    for i in range(10):
        ig.add((URIRef(f"https://example.org/{ part }/{ i }")) for part in SPO)
    us.insert(ig, ns)
    checks("after-insert")

    print("done")


if __name__ == "__main__":
    main()
