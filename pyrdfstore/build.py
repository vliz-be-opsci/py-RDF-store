import logging

from pyrdfstore.store import MemoryRDFStore, RDFStore, URIRDFStore

log = logging.getLogger(__name__)


def create_rdf_store(*store_info) -> RDFStore:
    """Creates an rdf_store based on the passed non-None arguments.
    0 of those arguments, will yield a MemoryRDFStore, 1-2 will be passed as read_uri resp write_uri to URIRDFStore
    Anything beyond is unacceptable
    """
    store_info = [
        el for el in store_info if el is not None
    ]  # remove possible None values
    assert (
        len(store_info) <= 2
    ), "Too many arguments to create store {store_info=}"

    if len(store_info) == 0:
        return MemoryRDFStore()
    # else
    return URIRDFStore(*store_info)
