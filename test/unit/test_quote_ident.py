"""Unit tests for quote_ident."""

import pytest

from migres.schema.ddl import quote_ident


pytestmark = pytest.mark.unit


def test_quotes_normal_names():
    assert quote_ident("users") == "`users`"
    assert quote_ident("id") == "`id`"
    assert quote_ident("_private") == "`_private`"


def test_doubles_backticks():
    assert quote_ident("a`b") == "`a``b`"
    assert quote_ident("`weird`") == "```weird```"


def test_rejects_empty():
    with pytest.raises(ValueError, match="empty"):
        quote_ident("")


def test_rejects_none():
    with pytest.raises(ValueError, match="None"):
        quote_ident(None)
