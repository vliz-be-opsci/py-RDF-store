#! /usr/bin/env python
import pytest
from typing import Iterable
from conftest import TEST_INPUT_FOLDER, loadfilegraph, make_sample_graph
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.query import Result
from util4tests import log, run_single_test

from pyrdfstore.store import RDFStore, RDFStoreDecorator

SELECT_ALL_SPO = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . }"


# TODO define a decorator implementation with some feature (e.g. narrow down to one ng?)
class DecoratedStore(RDFStoreDecorator):
    pass


@pytest.mark.usefixtures("rdf_stores")
def test_fixtures(rdf_stores: Iterable[RDFStore]):
    for rdf_store in rdf_stores:
        rdf_store_type: str = type(rdf_store).__name__
        decostore = DecoratedStore()
        assert False, "TODO make this test do something."


if __name__ == "__main__":
    run_single_test(__file__)
