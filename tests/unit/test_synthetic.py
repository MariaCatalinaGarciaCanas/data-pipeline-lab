"""Pruebas de la fuente sintetica y de la configuracion sin API key (modos que no llaman a EIA)."""

from __future__ import annotations

import pytest

from pipeline_lab.config import ConfigError, load_settings
from pipeline_lab.synthetic import RESPONDENTS, TYPE_NAMES, SyntheticSource

WINDOW = {"start": "2026-11-10T00", "end": "2026-11-10T23"}


def test_same_seed_gives_identical_data() -> None:
    a = SyntheticSource(seed=7).get_data(**WINDOW, length=5000)
    b = SyntheticSource(seed=7).get_data(**WINDOW, length=5000)
    other = SyntheticSource(seed=8).get_data(**WINDOW, length=5000)

    assert [dict(r) for r in a.rows] == [dict(r) for r in b.rows]
    assert [dict(r) for r in a.rows] != [dict(r) for r in other.rows]


def test_has_the_shape_of_eia_930() -> None:
    page = SyntheticSource().get_data(**WINDOW, length=5000)

    assert page.total == 24 * len(RESPONDENTS) * len(TYPE_NAMES)
    first = dict(page.rows[0])
    assert set(first) == {
        "period",
        "respondent",
        "respondent-name",
        "type",
        "type-name",
        "value",
        "value-units",
    }
    assert first["value-units"] == "megawatthours"
    assert isinstance(first["value"], str)  # EIA entrega los valores como texto


def test_pagination_covers_everything_exactly_once() -> None:
    source = SyntheticSource()
    seen = []
    for offset in range(0, 400, 100):
        seen += [
            tuple(r.items()) for r in source.get_data(**WINDOW, offset=offset, length=100).rows
        ]

    assert len(seen) == len(set(seen)) == 384


def test_includes_valid_negative_interchange_and_no_other_negatives() -> None:
    rows = SyntheticSource().get_data(start="2026-11-10T00", end="2026-11-12T23", length=5000).rows
    negatives = {r["type"] for r in rows if r["value"] is not None and int(r["value"]) < 0}

    assert negatives == {"TI"}


def test_anomalies_add_a_null_a_non_numeric_value_and_a_duplicate() -> None:
    rows = SyntheticSource(anomalies=True).get_data(**WINDOW, length=5000).rows

    assert any(r["value"] is None for r in rows)
    assert any(r["value"] == "n/a" for r in rows)
    keys = [(r["period"], r["respondent"], r["type"]) for r in rows]
    assert len(keys) - len(set(keys)) == 1


def test_unknown_facets_return_no_rows_like_eia() -> None:
    page = SyntheticSource().get_data(**WINDOW, facets={"respondent": ["NOPE"]})

    assert (page.total, page.rows) == (0, ())


@pytest.mark.parametrize("kwargs", [{"offset": -1}, {"length": 0}, {"length": 5001}])
def test_rejects_out_of_range_pagination(kwargs: dict) -> None:
    with pytest.raises(ValueError):
        SyntheticSource().get_data(**WINDOW, **kwargs)


# ---- configuracion sin API key
def test_settings_without_key_are_allowed_when_not_required() -> None:
    settings = load_settings(env={}, dotenv_path=None, require_api_key=False)

    assert settings.api_key.reveal() == ""


def test_the_key_is_still_required_by_default() -> None:
    with pytest.raises(ConfigError, match="EIA_API_KEY"):
        load_settings(env={}, dotenv_path=None)


def test_a_present_key_is_used_even_when_not_required() -> None:
    settings = load_settings(env={"EIA_API_KEY": "abc"}, dotenv_path=None, require_api_key=False)

    assert settings.api_key.reveal() == "abc"
