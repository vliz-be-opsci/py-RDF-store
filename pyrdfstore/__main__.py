import argparse
from pyrdfstore import store
from pyrdfstore.store import TargetStore, MemoryTargetStore, URITargetStore
import logging
import logging.config
import os

log = logging.getLogger(__name__)


def get_arg_parser():
    parser = argparse.ArgumentParser(
        description="A python library for interacting with RDF stores",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )

    parser.add_argument(
        "-m",
        "--mode",
        choices=["memorystore", "uristore"],
        default="memorystore",
        required=True,
        help="The mode to use for the store",
    )

    parser.add_argument(
        "-ts",
        "--target-store",
        required=False,
        help="A pair of URLS for the Targetstore to harvest from. The first is the url to get statments from , the second one is to post statements to.",
    )

    parser.add_argument(
        "-i",
        "--input",
        required=False,
        help="The input file to read from",
    )

    return parser


class TargetStoreFactory:

    def __init__(self):
        self.parser = get_arg_parser()

    @staticmethod
    def create_target_store(mode: str, target_store: str) -> TargetStore:
        if mode == "memorystore":
            return MemoryTargetStore()
        if mode == "uristore":
            return URITargetStore(target_store)
        raise ValueError("Invalid mode")


def main():
    args = get_arg_parser().parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    log.debug("Starting up")
    factory = TargetStoreFactory()
    target_store = factory.create_target_store(args.mode, args.target_store)
    if args.input:
        with open(args.input, "r") as f:
            data = f.read()
            target_store.insert(data)
    log.debug("Shutting down")
