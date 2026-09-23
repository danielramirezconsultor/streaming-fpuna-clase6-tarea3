"""Pruebas adicionales para la Tarea 3.

Complementan la suite provista por la cátedra. En particular, TestStream hace
visible la diferencia entre event time y orden de llegada y reifica PaneInfo.
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.windowed_value import PaneInfoTiming


class CapturePaneInfo(beam.DoFn):
    """Convertir la metadata implícita de Beam en campos verificables."""

    def process(
        self,
        element,
        window=beam.DoFn.WindowParam,
        pane_info=beam.DoFn.PaneInfoParam,
    ):
        key, total = element
        yield {
            "key": key,
            "total": total,
            "window_start": window.start.micros // 1_000_000,
            "window_end": window.end.micros // 1_000_000,
            "pane_timing": PaneInfoTiming.to_string(pane_info.timing),
            "pane_index": pane_info.index,
            "pane_is_first": pane_info.is_first,
            "pane_is_last": pane_info.is_last,
        }


def _late_stream():
    return (
        BeamTestStream()
        .advance_watermark_to(0)
        .add_elements([TimestampedValue(("m-a", 10), 5)])
        .advance_watermark_to(60)
        .add_elements([TimestampedValue(("m-a", 20), 50)])
        .advance_watermark_to_infinity()
    )


def test_teststream_accepts_late_revision_with_accumulating_panes(solution):
    options = PipelineOptions(["--streaming"])
    with BeamTestPipeline(options=options) as pipeline:
        output = (
            pipeline
            | _late_stream()
            | solution.build_trigger_policy(
                window_seconds=60,
                allowed_lateness_seconds=120,
            )
            | beam.CombinePerKey(sum)
        )

        # Si el late fuese descartado, nunca aparecería el total acumulado 30.
        def contains_late_revision(actual):
            assert ("m-a", 30) in list(actual)

        assert_that(output, contains_late_revision)


def test_teststream_exposes_window_and_late_pane_metadata(solution):
    options = PipelineOptions(["--streaming"])
    with BeamTestPipeline(options=options) as pipeline:
        observed = (
            pipeline
            | _late_stream()
            | solution.build_trigger_policy(
                window_seconds=60,
                allowed_lateness_seconds=120,
            )
            | beam.CombinePerKey(sum)
            | beam.ParDo(CapturePaneInfo())
        )

        def has_late_pane(actual):
            rows = list(actual)
            late_rows = [
                row
                for row in rows
                if row["key"] == "m-a"
                and row["total"] == 30
                and row["window_start"] == 0
                and row["window_end"] == 60
                and row["pane_timing"] == "LATE"
            ]
            assert late_rows, f"No se encontró el pane late esperado: {rows!r}"
            row = late_rows[0]
            assert isinstance(row["pane_index"], int)
            assert isinstance(row["pane_is_first"], bool)
            assert isinstance(row["pane_is_last"], bool)

        assert_that(observed, has_late_pane)


def test_same_event_id_in_same_merchant_is_emitted_once(solution):
    events = [
        ("m-a", {"event_id": "dup"}),
        ("m-a", {"event_id": "dup"}),
    ]

    with BeamTestPipeline() as pipeline:
        output = (
            pipeline
            | beam.Create(events)
            | beam.WindowInto(beam.window.FixedWindows(60))
            | beam.ParDo(solution.DeduplicatePayments())
            | beam.Map(lambda item: (item[0], item[1]["event_id"]))
        )
        assert_that(output, equal_to([("m-a", "dup")]))
