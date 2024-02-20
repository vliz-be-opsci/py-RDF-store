from pyrdfstore.store import (
    RDFStore,
    MemoryRDFStore,
    URIRDFStore,
)
from pyrdfstore.common import QUERY_BUILDER
from typing import Optional, List
from pyrdfj2 import J2RDFSyntaxBuilder
import os
import logging

log = logging.getLogger(__name__)


def create_rdf_store(rdf_store_info: Optional[List[str]] = None) -> RDFStore:
    if rdf_store_info != None:
        log.debug(len(rdf_store_info))
        assert (
            len(rdf_store_info) > 2
        ), "Invalid number of arguments. Max 2 arguments are allowed"

    if rdf_store_info == None:
        return MemoryRDFStore()
    # else
    if len(rdf_store_info) == 1:
        rdf_store_info.append(rdf_store_info[0])
    return URIRDFStore(
        read_uri=rdf_store_info[0],
        write_uri=rdf_store_info[1],
    )
