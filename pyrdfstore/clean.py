import logging
import os
import re
from enum import Enum
from functools import reduce
from typing import Callable, Iterable
from urllib.parse import quote

import validators
from rdflib import BNode, Graph, Literal, URIRef
from urnparse import URN8141, InvalidURNFormatError

log = logging.getLogger(__name__)


class Level(Enum):
    Graph = "graph"
    Triple = "triple"
    Node = "node"


def reparse(g: Graph, format="nt"):
    """This is a hack workaround for issue
    https://github.com/RDFLib/rdflib/issues/2760
    It reproduces the graph by serializing and parsing it again
    Via an intermediate format (not jsonld!) that is known to work

    :param g: the graph to reparse
    :param format: the intermediate format to use
    """
    log.debug("reparse called")
    return Graph().parse(data=g.serialize(format=format), format=format)


reparse.level = Level.Graph


def check_valid_urn(urn: str) -> bool:
    """Checks if the urn is valid (follows format rules)

    :param urn: the uri to check
    :type urn: str
    :return: True of urn is ok, else False"""
    try:
        URN8141.from_string(urn)
        return True
    except InvalidURNFormatError:
        return False


def check_valid_url(url: str) -> bool:
    """Checks if the url is valid (follows format rules)

    :param url: the url to check
    :type url: str
    :return: True if url is ok, else False"""
    return bool(validators.url(url))


def check_valid_uri(uri: str) -> bool:
    """Checks if the uri is valid (follows format rules)
    note that URI can be either of type URN or URL. So this
    will recognise which one and use the corresponding check

    :param uri: the uri to check
    :type uri: str
    :return: True if uri is ok, else False"""
    return bool(
        (uri.startswith("urn:") and check_valid_urn(uri))
        or check_valid_url(uri)
    )


def clean_uri_str(uri: str, smart: bool = False) -> str:
    """Escapes unacceptable chars in a URI.
    :param smart: (optional) flag indicating smart-mode of operation.
      Being 'smart' indicates the routine will only clean if it is
      needed. In other words: if True this does no cleaning in case the
      uri checks out to be already valid
      - defaults to False, so it executes a forced cleaning
      Note that smart=True should make the result idempotent,
      the non-smart-mode does not offer that guarantee.
      Both modes guarantee a valid uri output though.
    """
    if smart and check_valid_uri(uri):
        return uri
    # else
    return quote(uri, safe="~@#$&()*!+=:;,?/'")


def clean_uri_node(ref: URIRef | BNode | Literal) -> URIRef | BNode | Literal:
    """Escapes unacceptable chars in the URI of Graph Nodes
    This by nature only affects Nodes of type URIRef.
    """
    log.debug("clean_uri_node called")
    if not isinstance(ref, URIRef):
        return ref  # nothing to do if not URIRef
    # else
    uri = str(ref)
    if check_valid_uri(uri):
        return ref  # nothing to do if uri is valid
    # else
    return URIRef(clean_uri_str(uri))


clean_uri_node.level = Level.Node


def normalise_scheme_str(
    uri: str,
    domain: str | None = "schema.org",
    to_scheme: str | None = "https",
) -> str:
    # check uri matches ^https?://«domain».*
    # if not return input, else
    # replace ^https? part with desired to_scheme
    pattern = rf"^https?://{domain}"
    fixed_form = rf"{to_scheme}://{domain}"
    log.debug(f"substition of {pattern} into {fixed_form} on {uri}")
    return re.sub(pattern, fixed_form, uri)


def normalise_scheme_node(
    ref: URIRef | BNode | Literal,
    domain: str | None = "schema.org",
    to_scheme: str | None = "https",
) -> URIRef | BNode | Literal:
    log.debug("normalise_scheme_node called")
    if not isinstance(ref, URIRef):
        return ref  # nothing to do if not URIRef
    # else
    uri = str(ref)
    return URIRef(
        normalise_scheme_str(uri, domain=domain, to_scheme=to_scheme)
    )


normalise_scheme_node.level = Level.Node


NAMED_CLEAN_FUNCTIONS: dict = {
    "graph:reparse": reparse,
    "node:clean_uri": clean_uri_node,
    "node:normalise_schema.org": normalise_scheme_node,
}


def build_clean_chain(*specs) -> Callable:
    """
    converts a list of specifications for filters into
    a callable cleaner that can be applied (repeatedly) on multiple Graphs
    """
    assert specs, "No specs provided, no clean_chain to build"
    log.debug(f"building chain from {specs=}")
    # convert names to functions, and filter for fitting functions
    specs_fn = [
        spec if callable(spec) else NAMED_CLEAN_FUNCTIONS.get(str(spec), None)
        for spec in specs
    ]
    log.debug(f"specs as funtions {specs_fn=}")
    chain_fn = list(
        filter(
            lambda spec: spec is not None and hasattr(spec, "level"), specs_fn
        )
    )
    log.debug(f"chain of funtions {chain_fn=}")
    # group per level
    grouped_fn = reduce(  # group chain of cleaners per level
        lambda d, fn: d.get(fn.level, list()).append(fn) or d,
        chain_fn,  # list of functions to run over
        {lvl: list() for lvl in Level},  # inital dict of empty [] per level
    )
    log.debug(f"done grouping: {grouped_fn=}")
    remaining: int = reduce(
        lambda sum, lvl_list: sum + len(lvl_list),  # aggregate list lengths
        grouped_fn.values(),  # of all the lists associated to the levels
        0,
    )
    assert remaining > 0, (
        f"No remaining {remaining} filters on any level. "
        "Bad cleaning specs provided."
    )

    node_chain: list = grouped_fn[Level.Node]  # all the node-level functions
    log.debug(f"building {node_chain=}")
    if len(node_chain) > 0:

        def apply_node_chain(triple: tuple) -> tuple:
            # applies the node level cleaning functions
            log.debug("apply node-level chain cleaning")
            return tuple(  # returns a recreated triple
                (  # with the cleaned nodes
                    reduce(  # by
                        lambda n, node_fn: node_fn(n),  # chain-applying
                        node_chain,  # all the node-functions
                        node,  # on the input nodes
                    )
                    for node in triple  # coming from the input triple
                )
            )

        # note this by itself is a triple-level function
        apply_node_chain.level = Level.Triple
        # that can be added at the end of that chain
        grouped_fn[Level.Triple].append(apply_node_chain)

    triple_chain: list = grouped_fn[Level.Triple]  # all fn @triple-level
    log.debug(f"building {triple_chain=}")
    if len(triple_chain) > 0:

        def apply_triple_chain(graph: Graph) -> Graph:
            log.debug("apply triple-level chain cleaning")
            # applies the triple level cleaning functions
            clean: Graph = Graph()  # to a duplicate of the input graph
            for triple in graph.triples((None, None, None)):  # reassembled
                clean.add(  # from all triples
                    reduce(  # after
                        lambda t, triple_fn: triple_fn(t),  # chain-applying
                        triple_chain,  # all the triple-level-functions
                        triple,  # on them
                    )
                )
            return clean

        # note this by itself this is a graph-level function
        apply_triple_chain.level = Level.Graph
        # that can be added at the end of that chain
        grouped_fn[Level.Graph].append(apply_triple_chain)

    graph_chain: list = grouped_fn[Level.Graph]  # all graph-level-functions
    log.debug(f"building {graph_chain=}")
    assert len(graph_chain) > 0, (
        "No resulting actual filtering to do. "
        "Must be bug or bad specs provided."
    )

    def cleaner(graph: Graph) -> Graph:
        log.debug("main graph-level cleaning")
        return reduce(  # works by
            lambda g, graph_fn: graph_fn(g),  # chain-applying
            graph_chain,  # all graph-level-functions
            graph,  # initial value is the passed node
        )

    cleaner.level = Level.Graph
    return cleaner


def default_cleaner() -> Callable:
    fallback_specs: str = NAMED_CLEAN_FUNCTIONS.keys()  # all known cleaning
    specs: str = os.getenv("RDFSTORE_CLEANSPECS", fallback_specs)
    return build_clean_chain(*specs)


def clean_graph(graph: Graph, *specs: Iterable[str | Callable]) -> Graph:
    """
    Cleans the graph based on the provided "cleaning specifications"

    :param graph: to be cleaned
    :param specs: list of cleaner-functions or strings that match
    known keys in the dictionary NAMED_CLEAN_FUNCTIONS.
    These functions need to provide an attribute .level that
    uses one of the values in the Level Enum to indicate on what level
    they work. Depending on that level the function signature should
    match:
    - fn(graph: Graph) -> Graph:
    - fn(triple: tuple) -> tuple:
    - fn(node: URIRef| BNode | Literal) -> URIRef | BNode | Literal:
    """
    if not specs:
        return graph  # nothing to do
    # else
    if len(specs) == 1 and callable(specs[0]):
        return specs[0](graph)  # reuse a pre-built cleaner
    # else build cleaner from specs and run it
    cleaner = build_clean_chain(*specs)
    return cleaner(graph)
