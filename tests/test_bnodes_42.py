#! /usr/bin/env python
from typing import Iterable
from pathlib import Path
from requests import get
from uuid import uuid4

import pytest
from conftest import (
    DCT_ABSTRACT,
    TEST_INPUT_FOLDER,
    assert_file_ingest,
    loadfilegraph,
    make_sample_graph,
)
from rdflib import BNode, Graph, Literal
from rdflib.query import Result
from util4tests import log, run_single_test

from pyrdfstore.store import RDFStore


TEST_BASE: str = "https://marineinfo.org/id/publication/365467"


def file_content(input: Path):
    return input.read_text()


def web_content(url: str):
    accept = {"Accept": "text/turtle"}
    resp = get(url, headers=accept)
    return resp.text


def tops(text: str, *, num: int = 3, start: str = '_', sep: str = '\n'):
    """ get top (num) lines in sorted text that start as indicated
    :param text: text to filter, sort and top
    :param num: (int) number of lines to keep
    :param start: (char) matching start char of lines to keep in filter
      - default '_'
    :param sep: (char) used for marking line boundaries in text, 
      used in split and join
      - default '\n'
    """
    lines = [line for line in text.split(sep) if line.startswith(start)]
    lines.sort()
    return sep.join(lines[0:num])


def prepare_content(base, *filenames):
    if not filenames:
        filenames = (
            "marineinfo-publication-365467_nobase.ttl",
            "marineinfo-publication-365467.ttl",
        )
    content_dict = dict()
    for fn in filenames:
        fp: Path = TEST_INPUT_FOLDER / fn
        content_dict[fn] = file_content(fp)
    content_dict[base] = web_content(base)
    return content_dict


def test_graph_pubID_effect():
    base: str = TEST_BASE

    content_dict = prepare_content(base)
    # load our text content from different locations

    for label, content in content_dict.items():
        log.debug(f"using {label=} content")
        # parse without base and again without
        for pubid in (None, base):
            log.debug(f"using {pubid=}")
            g = Graph()
            log.debug(f".. parsing {label} content with {pubid=}")
            g.parse(data=content, format="turtle", publicID=pubid)
            ser_preskol = g.serialize(format="nt")
            tops_preskol = tops(ser_preskol)

            log.debug(f"... {label}, {pubid} -> \n{tops_preskol}\n<-")

            log.debug(".. now skolemizing")
            g.skolemize()
            ser_postskol = g.serialize(format="nt")
            tops_postkol = tops(ser_postskol)
            log.debug(f"... {label}, {pubid=}, skolem -> \n{tops_postkol}\n<-")

            # we do no think skolemization will have an effect
            assert tops_postkol == tops_preskol


@pytest.mark.usefixtures("rdf_stores")
def test_storing_content_with_bnodes(rdf_stores: Iterable[RDFStore]):
    base: str = TEST_BASE
    getpersons: str = (
        "prefix schema: <https://schema.org/>"
        "select distinct ?s "
        "where { ?s a schema:Person .}"
    )
    for rdf_store in rdf_stores:
        ng = f"urn:test:bnode42:{uuid4()}"
        rdf_store_type = type(rdf_store).__name__
        log.debug(f"for {rdf_store_type=}")
        numpersons_in_file = 23
        countloads = 0
        for label, content in prepare_content(base).items():
            log.debug(f". for input {label=}")
            for pubid in (None, base):
                log.debug(f".. for base {pubid=}")
                log.debug(f"... so combined {rdf_store_type}.insert(Graph.parse(data={label}, {pubid=}))")
                g = Graph()
                g.parse(data=content, format="turtle", publicID=pubid)
                rdf_store.insert(g, ng)
                countloads += 1
        persons: Result = rdf_store.select(getpersons, ng)
        log.debug(f"num persons = {len(persons)}")
        assert len(persons) == countloads * numpersons_in_file


@pytest.mark.usefixtures("rdf_stores")
def test_file_with_blanknodes_multiple_graphs(rdf_stores: Iterable[RDFStore]):
    """specific test for issue #42
    making sure distinct blanknodes are indeed considered separate after ingest
    even if loaded from different identical files
    """
    N: int = 3  # creating 3 distinct identical graphs
    graphs: Iterable[Graph] = tuple(
        loadfilegraph(TEST_INPUT_FOLDER / "issue-42.jsonld", format="json-ld")
        for i in range(N)
    )
    num_persons_in_file = 3
    ns: str = f"urn:test-jsonld-bnodes-42:{uuid4()}"
    uniquepersons: str = (
        "prefix schema: <https://schema.org/>"
        "select distinct ?p "
        "where { ?p a schema:Person .}"
    )

    for rdf_store in rdf_stores:
        rdf_store_type = type(rdf_store).__name__
        for g in graphs:
            rdf_store.insert(g, ns)
        result = rdf_store.select(uniquepersons, ns)
        assert len(result) == N * num_persons_in_file, (
            f"{rdf_store_type} :: "
            f"issue/42 unexpected response length {len(result)=} "
            f"not {N*num_persons_in_file=}"
        )
        log.debug(
            f"{rdf_store_type} :: no issue/42 detected {uniquepersons=} "
            f"and got {len(result)=}"
        )


if __name__ == "__main__":
    run_single_test(__file__)
