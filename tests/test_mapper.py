#! /usr/bin/env python
""" test_key_mapper
tests our expectations on translating identifying key objects
to/from named_graphs uri-strings
"""
from util4tests import run_single_test

from pyrdfstore import GraphNameMapper
from pyrdfstore.clean import check_valid_uri


def test_key_to_ng():
    base: str = "urn:test:"
    nmapper: GraphNameMapper = GraphNameMapper(base=base)

    # Test case 1: Valid filename
    key = "example.txt"
    expected_ng = base + "example.txt"
    ng = nmapper.key_to_ng(key)
    assert ng == expected_ng
    assert check_valid_uri(ng)

    # Test case 2: Filename with special characters
    key = "file name with spaces.txt"
    expected_ng = base + "file%20name%20with%20spaces.txt"
    ng = nmapper.key_to_ng(key)
    assert ng == expected_ng
    assert check_valid_uri(ng)

    # Test case 3: Path Like segments
    key = "c:/some/path/whatever.ext"
    expected_ng = base + key
    ng = nmapper.key_to_ng(key)
    assert ng == expected_ng
    assert check_valid_uri(ng)

    # Test case 4: URL-Like segments
    key = "http://example.org/path/whatever.ext"
    expected_ng = base + key
    ng = nmapper.key_to_ng(key)
    assert ng == expected_ng
    assert check_valid_uri(ng)

    # Test case 5: Empty key
    # TODO - reconsider --> would be better to throw an assertion in fact!
    key = ""
    expected_ng = base
    ng = nmapper.key_to_ng(key)
    assert ng == expected_ng
    assert check_valid_uri(ng)

    # Add more test cases as needed


def test_ng_to_key():
    base: str = "urn:test:"
    nmapper: GraphNameMapper = GraphNameMapper(base=base)

    # Test case 1: Valid named graph URN
    ng = base + "example.txt"
    expected_key = "example.txt"
    key = nmapper.ng_to_key(ng)
    assert key == expected_key

    # Test case 2: Named graph URN with special characters
    ng = base + "file%20name%20with%20spaces.txt"
    expected_key = "file name with spaces.txt"
    key = nmapper.ng_to_key(ng)
    assert key == expected_key


if __name__ == "__main__":
    run_single_test(__file__)
