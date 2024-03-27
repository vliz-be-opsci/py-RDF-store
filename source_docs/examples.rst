Examples
========

Script Example
--------------

.. code-block:: python

    from pyrdfstore import RDFStore, create_rdf_store

    # Parameters for the RDF store
    # if you have a local store running like graphdb or an endpoint 
    # you can query to you can give the read and write uri

    READ_URI = "http://localhost:7200/repositories/test"
    WRITE_URI = "http://localhost:7200/repositories/test/statements"

    # Create the RDF store

    rdf_store = create_rdf_store(READ_URI, WRITE_URI)

    # Query the RDF store

    sparql_query = """
    SELECT ?s ?p ?o
    WHERE {
        ?s ?p ?o
    }
    LIMIT 10
    """

    results = rdf_store.select(sparql_query)

    for result in results:
        print(result)

Next to select you can also use the following methods:

- insert : insert a rdflib.Graph object
- drop_graph : drop a named graph (removes triples from store)
- forget_graph : forget a named graph (does not remove triples from store)
- lastmod_ts : get the last modification timestamp of the store

If no read and write uri are given, the RDF store will be created with a temporary store in memory.
