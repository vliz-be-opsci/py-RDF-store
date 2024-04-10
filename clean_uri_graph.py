#! /usr/bin/env python
from rdflib import Graph, URIRef, Literal, BNode
from urllib.parse import quote
import validators


def check_valid_uri(uri: str) -> bool:
    return bool(validators.url(uri))


def clean_uri(uri: str) -> str:
    return quote(uri, safe='~@#$&()*!+=:;,?/\'')


def clean_node(ref: URIRef | BNode | Literal) -> URIRef | BNode | Literal:
    if not isinstance(ref, URIRef):
        return ref  # nothing to do if not URIRef
    # else
    uri = str(ref)
    if check_valid_uri(uri):
        return ref  # nothing to do if uri is valid
    # else
    return URIRef(clean_uri(uri))


def clean_graph(bgraph: Graph) -> Graph:
    cgraph: Graph = Graph()
    for btriple in bgraph.triples(tuple((None, None, None))):  # all triples
        ctriple = tuple((clean_node(node) for node in btriple))
        cgraph.add(ctriple)
    return cgraph


def main():
    baduri = "https://example.org/this(ok/this[notso];so_yeah)"
    valid = check_valid_uri(baduri)
    print(f"expected bad uri was {valid=}")

    gd_uri = clean_uri(baduri)
    valid = check_valid_uri(gd_uri)
    print(f"expected bad uri was {valid=}")

    badgraph = Graph().add(tuple((URIRef(baduri), BNode(), Literal("yo"))))
    badgraph.add(tuple((BNode(), URIRef(baduri), Literal("no"))))
    badgraph.add(tuple((URIRef(baduri), URIRef(baduri), URIRef(baduri))))
    badgraph.add(tuple((URIRef(gd_uri), URIRef(gd_uri), URIRef(gd_uri))))
    print(badgraph.serialize(format="nt"))

    gd_graph = clean_graph(badgraph)
    print(gd_graph.serialize(format="nt"))

    print(f"{len(badgraph)=} | {len(gd_graph)=}")

    print("done")


if __name__ == '__main__':
    main()
