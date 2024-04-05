#! /usr/bin/env python
from random import randint
from typing import Iterable
from uuid import uuid4

import pytest
from conftest import make_sample_graph
from rdflib import Graph
from rdflib.query import Result
from util4tests import log, run_single_test

from pyrdfstore.store import RDFStore, RDFStoreDecorator

SELECT_ALL_SPO = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . }"


class DecoratedStore(RDFStoreDecorator):
    def __init__(self, rdf_store, ng):
        super().__init__(rdf_store)
        self._ng = ng

    def select_ng(self, sparql):
        return self.select(sparql, self._ng)

    def insert_ng(self, graph):
        return self.insert(graph, self._ng)

    def all_ng(self):
        return self.select_ng(SELECT_ALL_SPO)


@pytest.mark.usefixtures("rdf_stores")
def test_fixtures(rdf_stores: Iterable[RDFStore]):
    size: int = randint(3, 10)
    base: int = randint(0, 5)
    sg: Graph = make_sample_graph(range(base, base + size))
    my_urn: str = f"urn:decorator:my:{uuid4()}"
    for rdf_store in rdf_stores:
        rdf_store_type: str = type(rdf_store).__name__
        decostore = DecoratedStore(rdf_store, my_urn)

        decostore.insert_ng(sg)
        all: Result = decostore.all_ng()
        log.debug(f"{rdf_store_type} :: got {len(all)=}")
        assert len(all) == size, f"{rdf_store_type} :: not all triples found?"


if __name__ == "__main__":
    run_single_test(__file__)
