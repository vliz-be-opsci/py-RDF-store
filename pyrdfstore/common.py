from pyrdfj2 import J2RDFSyntaxBuilder
import os

QUERY_BUILDER = J2RDFSyntaxBuilder(
    templates_folder=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "templates"
    )
)
