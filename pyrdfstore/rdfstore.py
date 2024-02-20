from pyrdfstore.store import (
    RDFStore,
    MemoryRDFStore,
    URIRDFStore,
    RDFStoreAccess,
)
from pyrdfstore.common import QUERY_BUILDER
from typing import Optional, List
from pyrdfj2 import J2RDFSyntaxBuilder
import os


def create_rdf_store(rdf_store_info: Optional[List[str]] = None) -> RDFStore:
    assert (
        len(rdf_store_info) > 2
    ), "Invalid number of arguments. Max 2 arguments are allowed"

    if rdf_store_info == None:
        return MemoryRDFStore()

    if len(rdf_store_info) == 1:
        return URIRDFStore(
            qryBuilder=QUERY_BUILDER,
            read_uri=rdf_store_info[0],
            write_uri=rdf_store_info[0],
        )

    return URIRDFStore(
        qryBuilder=QUERY_BUILDER,
        read_uri=rdf_store_info[0],
        write_uri=rdf_store_info[1],
    )
