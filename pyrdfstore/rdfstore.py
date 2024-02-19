from pyrdfstore.store import TargetStore, MemoryTargetStore, URITargetStore
from typing import Optional, List
from pyrdfj2 import J2RDFSyntaxBuilder
import os


class TargetStoreFactory:
    """
    This class is used to create a target store based on the mode and target store URLs provided.

    :param mode: The mode to use to create the target store. The mode can be either 'memorystore' or 'uristore'
    :type mode: str
    :param target_store_urls: The URLs of the target store. The first URL is the URL to get statements from, and the second URL is the URL to post statements to.
    :type target_store_urls: Optional[List[str]]
    :param input_file: The input file to use to create the target store
    :type input_file: Optional[str]
    """

    def __init__(
        self,
        mode: str,
        target_store_urls: Optional[List[str]] = None,
        input_file: Optional[str] = None,
    ):
        self.mode = mode
        self.target_store_urls = target_store_urls
        self.input_file = input_file

        if (
            self.mode == "uristore"
            and not self.target_store_urls
            or len(self.target_store_urls) < 1
            or len(self.target_store_urls) > 2
        ):
            raise ValueError(
                "Target store must be provided for uristore mode. The target store must be a list of 2 URLs. The first URL is the URL to get statements from, and the second URL is the URL to post statements to."
            )

    def create_target_store(self) -> TargetStore:
        """Create a target store based on the mode and target store URLs provided.
        :return: A target store instance
        :rtype: TargetStore
        """

        querybuilder = J2RDFSyntaxBuilder(
            templates_folder=os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "templates"
            )
        )

        if self.mode == "memorystore":
            return MemoryTargetStore()
        if self.mode == "uristore":
            return URITargetStore(
                qryBuilder=querybuilder,
                read_uri=self.target_store_urls[0],
                write_uri=self.target_store_urls[1],
            )
        raise ValueError("Invalid mode")
