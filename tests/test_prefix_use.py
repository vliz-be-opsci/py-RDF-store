#! /usr/bin/env python
from typing import Iterable
from uuid import uuid4

import pytest
from conftest import make_sample_graph
from rdflib import Graph
from rdflib.query import Result
from util4tests import log, run_single_test

from pyrdfstore.store import RDFStore


@pytest.mark.usefixtures("rdf_stores")
def test_sparql_with_regex_and_prefix(rdf_stores: Iterable[RDFStore]):
    """specific test for issue #29
    making sure sparql statements with prefix and regex statements are working
    """
    lbl: str = "issue-29"
    base: str = f"https://example.org/base-{lbl}/"
    num: int = 5
    start: int = 100
    g: Graph = make_sample_graph(range(start, start + num), base)
    ns: str = f"urn:test:uuid:{uuid4()}"
    sparql: str = (
        f"prefix schema: <{base}>"
        "select *"
        "where { "
        "    [] ?p ?o . "
        f"    filter(regex(str(?p), '{lbl}'))"
        "}"
    )

    for rdf_store in rdf_stores:
        rdf_store_type = type(rdf_store).__name__
        rdf_store.insert(g, ns)
        result = rdf_store.select(sparql, ns)
        assert isinstance(result, Result), (
            f"{rdf_store_type} :: "
            "issue/29 cannot execute selects with prefix and regex parts"
        )
        assert len(result) == num, (
            f"{rdf_store_type} :: "
            f"issue/29 unexpected response length {len(result)=} not {num=}"
        )
        log.debug(
            f"{rdf_store_type} :: no issue/29 detected {sparql=} "
            f"and got {len(result)=}"
        )


if __name__ == "__main__":
    run_single_test(__file__)
