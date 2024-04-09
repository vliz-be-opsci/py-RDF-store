#! /usr/bin/env python
from typing import Iterable
from pathlib import Path
from requests import get
from uuid import uuid4

from conftest import loadfilegraph, TEST_INPUT_FOLDER, format_from_extension
import pytest
from rdflib import BNode, Graph, Literal, Namespace
from rdflib.query import Result
from util4tests import log, run_single_test
from rdflib.plugins.parsers.jsonld import JsonLDParser
from rdflib.parser import Parser
from pyrdfstore.store import RDFStore, reparse


SCHEMA: Namespace = Namespace("https://schema.org/")
RDF: Namespace = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
EX: Namespace = Namespace("http://example.org/")


def test_compare_bnodes_ttl_jsonld():
    localnames = ["b0", "b1", "x9"]
    tst_content = dict()

    ttl = "\n".join(f"_:{ln} a <{EX}MyType> ." for ln in localnames)
    tst_content['turtle'] = ttl

    js = ",\n  ".join(f"{{\"@id\": \"_:{ln}\", \"@type\": \"{EX.MyType}\" }}"for ln in localnames)
    tst_content['json-ld'] = f"[\n  {js} ]"

    for fmt, cntnt in tst_content.items():
        log.debug(f"{fmt=}, cntnt ->\n{cntnt}\n<-")
        g = reparse(Graph().parse(data=cntnt, format=fmt))

        items = list(g.subjects(predicate=RDF.type, object=EX.MyType))
        assert len(items) == 3
        for i, item in enumerate(items):
            log.debug(f". found {i} -> {type(item).__name__} {item.n3()=} ")
            assert isinstance(item, BNode)
            assert item.n3()[2:] not in localnames, f"problem with format {fmt}"


def test_compare_bnodes_ttl_jsonld_files():
    """testing how rdlib jsonld parser is handling the "@id": "_:n1" in json
    seems like an issue un rdflib that matches this problem in rdflib.js
    see: https://github.com/linkeddata/rdflib.js/issues/555
    """
    for fname in ("issue-42.jsonld", "issue-42.ttl"):
        localnames = ["b0", "b1", "x9"]  # known local id to be used in these files
        log.debug(f"testing ${fname=}")
        fpath = TEST_INPUT_FOLDER / fname

        g: Graph = reparse(loadfilegraph(fpath, format=format_from_extension(fpath)))
        persons = g.subjects(predicate=RDF.type, object=SCHEMA.Person)
        for i, p in enumerate(persons):
            log.debug(f". found {i} -> {type(p).__name__} {p.n3()=} ")
            assert isinstance(p, BNode)
            assert p.n3()[2:] not in localnames, "local id {p.n3()} used in file should have been replaced"


if __name__ == "__main__":
    run_single_test(__file__)
