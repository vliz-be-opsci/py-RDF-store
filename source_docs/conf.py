import os
import sys

from recommonmark.parser import CommonMarkParser

source_parsers = {
    ".md": CommonMarkParser,
}

source_suffix = [".rst", ".md"]
sys.path.insert(0, os.path.abspath("."))
# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "pyrdfstore"
copyright = "2024, VLIZ vzw"
author = "Cedric Decruw"
version = "0.1.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosummary",
    "sphinx.ext.mathjax",
    "sphinx.ext.githubpages",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns = []

# The master toctree document.
# master_doc = "pyTravHarv/index"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
