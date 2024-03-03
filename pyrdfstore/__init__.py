"""pyrdfstore

.. module:: pyrdfstore
:platform: Unix, Windows
:synopsis: A library for creating and interacting with RDF stores

.. moduleauthor:: Vliz VZW open science <opsci@vliz.be>
"""

from .build import create_rdf_store
from .store import RDFStore

__all__ = ["RDFStore", "create_rdf_store"]
