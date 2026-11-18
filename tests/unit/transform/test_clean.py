"""Pruebas de la limpieza Raw -> Clean (Polars): tipos, validacion, rechazos y duplicados."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from pipeline_lab.storage import rows_to_frame
from pipeline_lab.transform import (
    CLEAN_COLUMNS,
    SchemaError,
    clean_observations,
    write_clean,
)

T1 = datetime(2026, 11, 16, 10, 0, tzinfo=UTC)
T2 = datetime(2026, 11, 16, 11, 0, tzinfo=UTC)  # una ingesta posterior


def r(
    period="2026-11-10T05",
    respondent="CISO",
    type_="D",
    value="100",
    units="megawatthours",
    name="California ISO",
    type_name="Demand",
):  # noqa: PLR0913
    return {
        "period": period,
        "respondent": respondent,
        "respondent-name": name,
        "type": type_,
        "type-name": type_name,
        "value": value,
        "value-units": units,
    }


def raw(rows, ingested_at=T1) -> pl.DataFrame:
    return rows_to_frame(rows, ingested_at)


def reasons(result) -> dict[str, int]:
    return result.stats.rejected_by_reason


# ============================================================================ filas validas
def test_valid_rows_get_the_clean_schema_and_types() -> None:
    result = clean_observations(raw([r(value="27053")]))

    clean = result.clean
    assert clean.columns == list(CLEAN_COLUMNS)
    assert clean.schema["period_utc"] == pl.Datetime("us")  # sin zona: significa UTC
    assert clean.schema["observation_date"] == pl.Date
    assert clean.schema["hour_utc"] == pl.Int8
    assert clean.schema["value"] == pl.Float64
    assert clean.schema["is_negative_value"] == pl.Boolean
    assert clean.schema["ingested_at"] == pl.Datetime("us")
    assert clean.row(0, named=True) == {
        "period_utc": datetime(2026, 11, 10, 5),
        "observation_date": date(2026, 11, 10),
        "hour_utc": 5,
        "respondent": "CISO",
        "respondent_name": "California ISO",
        "type": "D",
        "type_name": "Demand",
        "category": "demand",
        "value": 27053.0,
        "value_units": "megawatthours",
        "is_negative_value": False,
        "ingested_at": datetime(2026, 11, 16, 10, 0),
    }
    assert result.rejected.height == 0


def test_the_four_types_map_to_distinct_categories() -> None:
    rows = [r(type_=t) for t in ("D", "DF", "NG", "TI")]

    clean = clean_observations(raw(rows)).clean.sort("type")

    assert dict(zip(clean["type"], clean["category"], strict=True)) == {
        "D": "demand",
        "DF": "demand_forecast",
        "NG": "generation",
        "TI": "interchange",
    }
    assert (
        dict(zip(clean["type"], clean["type_name"], strict=True))["DF"]
        == "Day-ahead demand forecast"
    )


def test_negative_interchange_is_valid_and_flagged_not_rejected() -> None:
    result = clean_observations(raw([r(type_="TI", value="-504"), r(type_="D", value="10")]))

    assert result.rejected.height == 0
    by_type = {row["type"]: row for row in result.clean.to_dicts()}
    assert by_type["TI"]["value"] == -504.0
    assert by_type["TI"]["is_negative_value"] is True
    assert by_type["D"]["is_negative_value"] is False


def test_zero_and_decimal_values_are_valid() -> None:
    result = clean_observations(raw([r(period="2026-11-10T01", value="0"), r(value="12.5")]))

    assert sorted(result.clean["value"].to_list()) == [0.0, 12.5]


def test_text_is_normalised_trimmed_and_uppercased() -> None:
    row = r(
        period=" 2026-11-10T05 ",
        respondent=" ciso ",
        type_=" d ",
        value=" 100 ",
        units=" MegaWattHours ",
    )

    clean = clean_observations(raw([row])).clean

    assert clean.row(0, named=True)["respondent"] == "CISO"
    assert clean.row(0, named=True)["type"] == "D"
    assert clean.row(0, named=True)["value"] == 100.0
    assert clean.row(0, named=True)["value_units"] == "megawatthours"


def test_missing_respondent_name_falls_back_to_the_code() -> None:
    clean = clean_observations(raw([r(name=None), r(period="2026-11-10T06", name="  ")])).clean

    assert set(clean["respondent_name"].to_list()) == {"CISO"}


def test_hour_and_date_are_derived_in_utc() -> None:
    clean = clean_observations(raw([r(period="2026-11-10T23"), r(period="2026-11-11T00")])).clean

    assert clean.sort("period_utc")["hour_utc"].to_list() == [23, 0]
    assert clean.sort("period_utc")["observation_date"].to_list() == [
        date(2026, 11, 10),
        date(2026, 11, 11),
    ]


# ================================================================================ rechazos
@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (r(period="2026-13-45T99"), "invalid_period"),
        (r(period="garbage"), "invalid_period"),
        (r(period=None), "invalid_period"),
        (r(period="2026-11-10"), "invalid_period"),
        (r(period="2026-02-31T05"), "invalid_period"),
        (r(respondent=""), "missing_respondent"),
        (r(respondent=None), "missing_respondent"),
        (r(type_="XX"), "unknown_type"),
        (r(type_=None), "unknown_type"),
        (r(value=None), "invalid_value"),
        (r(value=""), "invalid_value"),
        (r(value="n/a"), "invalid_value"),
        (r(value="nan"), "invalid_value"),
        (r(value="inf"), "invalid_value"),
        (r(value="-inf"), "invalid_value"),
        (r(units="kilowatthours"), "unexpected_units"),
        (r(units=None), "unexpected_units"),
    ],
)
def test_invalid_rows_are_rejected_with_the_reason_and_never_dropped_silently(row, reason) -> None:
    result = clean_observations(raw([row, r(period="2026-11-10T09")]))

    assert result.clean.height == 1  # solo la valida
    assert result.rejected.height == 1
    assert result.rejected["reject_reason"].to_list() == [reason]
    assert result.stats.raw_rows == result.stats.clean_rows + result.stats.rejected_rows


def test_rejected_rows_keep_the_original_values() -> None:
    result = clean_observations(raw([r(value=" n/a ", respondent=" Foo ")]))

    rejected = result.rejected.row(0, named=True)
    assert rejected["value"] == " n/a "  # sin recortar ni corregir: tal como llego
    assert rejected["respondent"] == " Foo "
    assert rejected["reject_reason"] == "invalid_value"
    assert "_ingested_at" in result.rejected.columns


def test_only_the_first_failing_rule_is_reported() -> None:
    result = clean_observations(raw([r(period="mal", type_="XX", value="n/a")]))

    assert result.rejected["reject_reason"].to_list() == ["invalid_period"]


def test_rejections_are_counted_by_reason() -> None:
    rows = [r(value=None), r(value="x", period="2026-11-10T01"), r(type_="XX"), r(period="mal")]

    stats = clean_observations(raw(rows)).stats

    assert stats.rejected_by_reason == {"invalid_period": 1, "invalid_value": 2, "unknown_type": 1}
    assert stats.clean_rows == 0


# =============================================================================== duplicados
def test_exact_duplicates_keep_one_row_and_reject_the_other() -> None:
    result = clean_observations(raw([r(), r()]))

    assert result.clean.height == 1
    assert reasons(result) == {"duplicate_superseded": 1}


def test_conflicting_duplicates_keep_the_most_recently_ingested() -> None:
    older = raw([r(value="100")], T1)
    newer = raw([r(value="150")], T2)

    result = clean_observations(pl.concat([older, newer]))

    assert result.clean["value"].to_list() == [150.0]
    assert result.rejected["value"].to_list() == ["100"]  # el desplazado se conserva, con motivo
    assert result.rejected["reject_reason"].to_list() == ["duplicate_superseded"]


def test_the_latest_wins_regardless_of_row_order() -> None:
    newer_first = pl.concat([raw([r(value="150")], T2), raw([r(value="100")], T1)])

    assert clean_observations(newer_first).clean["value"].to_list() == [150.0]


def test_different_keys_are_not_duplicates() -> None:
    rows = [r(), r(respondent="PJM"), r(type_="DF"), r(period="2026-11-10T06")]

    result = clean_observations(raw(rows))

    assert result.clean.height == 4
    assert result.rejected.height == 0


def test_invalid_rows_do_not_supersede_valid_ones() -> None:
    rows_old = raw([r(value="100")], T1)
    rows_new = raw([r(value="n/a")], T2)  # la ingesta nueva trae un valor invalido

    result = clean_observations(pl.concat([rows_old, rows_new]))

    assert result.clean["value"].to_list() == [100.0]  # se conserva la valida
    assert reasons(result) == {"invalid_value": 1}


def test_clean_keys_are_unique() -> None:
    rows = [r(period=f"2026-11-10T{h:02d}", respondent=c) for h in range(24) for c in ("A", "B")]
    rows += rows[:10]  # 10 duplicados

    clean = clean_observations(raw(rows)).clean

    assert clean.height == 48
    assert clean.select(["period_utc", "respondent", "type"]).is_duplicated().sum() == 0


# =============================================================================== estadisticas
def test_null_counts_are_reported_per_raw_column() -> None:
    stats = clean_observations(raw([r(value=None), r(period="2026-11-10T01", name=None)])).stats

    assert stats.null_counts["value"] == 1
    assert stats.null_counts["respondent-name"] == 1
    assert stats.null_counts["period"] == 0


def test_empty_input_gives_empty_outputs_with_the_right_schema() -> None:
    result = clean_observations(rows_to_frame([], T1))

    assert result.clean.height == 0
    assert result.clean.columns == list(CLEAN_COLUMNS)
    assert result.stats.raw_rows == 0


# ============================================================================ entrada y salida
def test_missing_columns_raise_a_clear_schema_error() -> None:
    with pytest.raises(SchemaError, match="value-units"):
        clean_observations(raw([r()]).drop("value-units"))


def test_accepts_a_lazyframe() -> None:
    result = clean_observations(raw([r(), r(period="2026-11-10T06")]).lazy())

    assert result.clean.height == 2


def test_cleaning_is_deterministic() -> None:
    rows = [r(period=f"2026-11-10T{h:02d}", value=str(h * 10)) for h in range(24)]
    rows += [r(value="n/a", period="2026-11-11T00"), r(period="mal")]

    first = clean_observations(raw(rows))
    second = clean_observations(raw(rows))

    assert first.clean.equals(second.clean)
    assert first.rejected.equals(second.rejected)


def test_write_clean_creates_both_files_and_round_trips(tmp_path: Path) -> None:
    result = clean_observations(raw([r(), r(value="n/a", period="2026-11-10T01")]))

    observations, rejected = write_clean(result, tmp_path / "clean")

    assert observations.name == "observations.parquet"
    assert rejected.name == "rejected.parquet"
    assert pl.read_parquet(observations).equals(result.clean)
    assert pl.read_parquet(rejected).equals(result.rejected)
    assert sorted(p.name for p in observations.parent.iterdir()) == [
        "observations.parquet",
        "rejected.parquet",
    ]
