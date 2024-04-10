"""pyrdfstore

.. module:: pyrdfstore
:platform: Unix, Windows
:synopsis: A library for creating and interacting with RDF stores

.. moduleauthor:: "Open Science Team VLIZ vzw" <opsci@vliz.be>
"""

from .build import create_rdf_store
from .store import RDFStore

__all__ = ["RDFStore", "create_rdf_store"]
