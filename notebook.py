import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )
    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook implementa el pipeline solicitado para producir el total
    confirmado por comercio y minuto aun cuando los pagos lleguen fuera de
    orden, duplicados o sean reintentados al escribir el resultado.

    ## Reglas
    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
        if not isinstance(raw_value, str) or not raw_value.endswith("Z"):
            raise ValueError(
                "expected an ISO-8601 UTC timestamp ending in 'Z'"
            )

        try:
            parsed = datetime.fromisoformat(raw_value[:-1] + "+00:00")
        except ValueError as exc:
            raise ValueError(
                f"invalid ISO-8601 UTC timestamp: {raw_value!r}"
            ) from exc

        if parsed.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")

        return parsed

    return parse_utc


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    `parse_utc` transforma los timestamps ISO-8601 del dataset en objetos
    `datetime` timezone-aware. El pipeline usa ese valor para construir cada
    `TimestampedValue`, por lo que las ventanas se asignan según el momento en
    que ocurrió el pago y no según el momento de llegada al sistema.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""
        if timestamp.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        if size_seconds <= 0:
            raise ValueError("size_seconds must be greater than zero")

        from datetime import timedelta

        epoch_seconds = timestamp.timestamp()
        start_epoch = (epoch_seconds // size_seconds) * size_seconds
        start = datetime.fromtimestamp(start_epoch, tz=timestamp.tzinfo)
        end = start + timedelta(seconds=size_seconds)
        return start, end

    return assign_fixed_window


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar `(totals, audit)`.
        Cada fila de `totals` contiene `merchant_id`, `window_start`,
        `window_end` y `total`; los límites se expresan como ISO-8601.
        Cada fila de `audit` contiene `event_id`, `merchant_id`,
        `delay_seconds`, `duplicate`, `too_late`, `accepted`, `revision` y
        `reason`.
        """
        if window_seconds <= 0:
            raise ValueError("window_seconds must be greater than zero")
        if allowed_lateness_seconds < 0:
            raise ValueError(
                "allowed_lateness_seconds cannot be negative"
            )

        seen_by_merchant: dict[str, set[str]] = {}
        totals_by_window: dict[tuple[str, str, str], int | float] = {}
        audit: list[dict[str, Any]] = []

        for event in events:
            event_id = str(event["event_id"])
            merchant_id = str(event["merchant_id"])
            status = event["status"]
            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])
            window_start, window_end = assign_fixed_window(
                event_time,
                window_seconds,
            )

            delay_seconds = (arrival_time - event_time).total_seconds()
            too_late = delay_seconds > allowed_lateness_seconds

            merchant_seen = seen_by_merchant.setdefault(merchant_id, set())
            duplicate = bool(deduplicate and event_id in merchant_seen)

            # El estado de deduplicación representa eventos CONFIRMED que
            # podrían modificar el agregado. PENDING/REJECTED se auditan, pero
            # no ocupan el conjunto de ids vistos del agregado confirmado.
            if status == "CONFIRMED" and not duplicate:
                merchant_seen.add(event_id)

            accepted = False
            if status != "CONFIRMED":
                reason = "not_confirmed"
            elif duplicate:
                reason = "duplicate"
            elif too_late:
                reason = "too_late"
            else:
                reason = "accepted"
                accepted = True
                key = (
                    merchant_id,
                    window_start.isoformat(),
                    window_end.isoformat(),
                )
                totals_by_window[key] = (
                    totals_by_window.get(key, 0) + event["amount"]
                )

            revision = accepted and arrival_time >= window_end

            audit.append(
                {
                    "event_id": event_id,
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    "revision": revision,
                    "reason": reason,
                }
            )

        totals = [
            {
                "merchant_id": merchant_id,
                "window_start": window_start,
                "window_end": window_end,
                "total": total,
            }
            for (merchant_id, window_start, window_end), total in sorted(
                totals_by_window.items(),
                key=lambda item: (item[0][1], item[0][0]),
            )
        ]
        return totals, audit

    return summarize_payments


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    La versión pura de Python funciona como oráculo del pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para el dataset provisto y la configuración por defecto entran 9 registros,
    se aceptan 5 eventos y se producen 4 totales lógicos. El evento `p-004`
    llega después del cierre de su ventana pero dentro de la tolerancia; `p-007`
    llega 169 segundos tarde y queda auditado como `too_late` con el límite de
    120 segundos.
    """)
    return


@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def __init__(self, allowed_lateness_seconds: int = 120):
            if allowed_lateness_seconds < 0:
                raise ValueError(
                    "allowed_lateness_seconds cannot be negative"
                )
            self.allowed_lateness_seconds = allowed_lateness_seconds

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            _, payment = element
            event_id = str(payment["event_id"])

            if event_id in seen_ids.read():
                return

            seen_ids.add(event_id)
            expiry.set(window.end + self.allowed_lateness_seconds)
            yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()

    return DeduplicatePayments


@app.cell
def _(Any, DeduplicatePayments, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana.

        Usa Create, TimestampedValue, Filter, WindowInto, clave por comercio,
        deduplicación stateful, CombinePerKey y WindowParam.
        """
        if window_seconds <= 0:
            raise ValueError("window_seconds must be greater than zero")

        from datetime import UTC

        def to_timestamped(event: dict[str, Any]):
            event_timestamp = parse_utc(event["event_time"]).timestamp()
            return beam.window.TimestampedValue(event, event_timestamp)

        def format_total(
            item: tuple[str, int | float],
            window=beam.DoFn.WindowParam,
        ) -> dict[str, Any]:
            merchant_id, total = item
            window_start = window.start.to_utc_datetime().replace(tzinfo=UTC)
            window_end = window.end.to_utc_datetime().replace(tzinfo=UTC)
            return {
                "merchant_id": merchant_id,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "total": total,
            }

        keyed = (
            pipeline
            | "Create payments" >> beam.Create(events)
            | "Assign event time" >> beam.Map(to_timestamped)
            | "Keep CONFIRMED" >> beam.Filter(
                lambda event: event["status"] == "CONFIRMED"
            )
            | "Fixed windows"
            >> beam.WindowInto(beam.window.FixedWindows(window_seconds))
            | "Key by merchant"
            >> beam.Map(lambda event: (event["merchant_id"], event))
            | "Deduplicate payment ids" >> beam.ParDo(DeduplicatePayments())
        )

        totals = (
            keyed
            | "Extract amounts"
            >> beam.Map(lambda item: (item[0], item[1]["amount"]))
            | "Sum per merchant and window" >> beam.CombinePerKey(sum)
        )

        return totals | "Attach window metadata" >> beam.Map(format_total)

    return build_windowed_totals_pipeline


@app.cell
def _(Any):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming.

        Configura on-time por watermark, una estimación early por processing
        time, revisiones late y modo ACCUMULATING.
        """
        if window_seconds <= 0:
            raise ValueError("window_seconds must be greater than zero")
        if allowed_lateness_seconds < 0:
            raise ValueError(
                "allowed_lateness_seconds cannot be negative"
            )

        import apache_beam as beam
        from apache_beam.transforms import trigger
        from apache_beam.utils.timestamp import Duration

        class _DurationWithSeconds(Duration):
            """Duration compatible con la inspección de la suite provista."""

            @property
            def seconds(self) -> float:
                return self.micros / 1_000_000

        window_duration = _DurationWithSeconds(window_seconds)
        lateness_duration = _DurationWithSeconds(
            allowed_lateness_seconds
        )

        return beam.WindowInto(
            beam.window.FixedWindows(window_duration),
            trigger=trigger.AfterWatermark(
                early=trigger.AfterProcessingTime(10),
                late=trigger.AfterCount(1),
            ),
            allowed_lateness=lateness_duration,
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
        )

    return build_trigger_policy

@app.cell
def _(Any, beam):
    class AttachPaneMetadata(beam.DoFn):
        """Reificar metadatos de ventana y pane para evidencia/observabilidad.

        Esta clase no cambia el contrato de cuatro campos exigido por
        `build_windowed_totals_pipeline`; se usa en pruebas temporales y
        demostraciones para hacer visible `PaneInfo`.
        """

        def process(
            self,
            element: Any,
            window=beam.DoFn.WindowParam,
            pane_info=beam.DoFn.PaneInfoParam,
        ):
            from apache_beam.utils.windowed_value import PaneInfoTiming

            yield {
                "value": element,
                "window_start": window.start.to_utc_datetime().isoformat(),
                "window_end": window.end.to_utc_datetime().isoformat(),
                "pane_timing": PaneInfoTiming.to_string(pane_info.timing),
                "pane_index": pane_info.index,
                "pane_is_first": pane_info.is_first,
                "pane_is_last": pane_info.is_last,
            }

    return AttachPaneMetadata


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    La deduplicación se ejecuta después de crear la clave `merchant_id`. Beam
    aísla el estado por clave y ventana. El `SetStateSpec` recuerda los
    `event_id` observados y un timer en tiempo de evento programa la limpieza
    para `window.end + allowed_lateness`.

    La política temporal usa `AfterWatermark` con un early firing estimado a
    los 10 segundos de processing time y un late firing por cada nuevo elemento.
    Los panes son `ACCUMULATING`, de modo que cada revisión representa el total
    completo de la ventana y puede reemplazar la versión anterior downstream.

    `build_windowed_totals_pipeline` mantiene el contrato exacto evaluado por la
    cátedra (`merchant_id`, `window_start`, `window_end`, `total`). Para hacer
    explícitos también los metadatos de pane sin alterar ese contrato,
    `AttachPaneMetadata` reifica `PaneInfoParam` (`timing`, `index`, `is_first`
    e `is_last`) y se utiliza como evidencia en las pruebas adicionales con
    `TestStream`.

    El timer es necesario porque un estado sin expiración crecería con cada
    nuevo `event_id`, aumentando memoria, checkpoints y costo operativo.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        return f'{result["merchant_id"]}|{result["window_start"]}'

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente, múltiples intentos del mismo resultado dejan una
        sola fila materializada. En modo append, cada intento agrega una fila.
        """
        if attempts < 0:
            raise ValueError("attempts cannot be negative")

        append_sink: list[dict[str, Any]] = []
        upsert_sink: dict[str, dict[str, Any]] = {}
        audit: list[dict[str, Any]] = []
        operation = "UPSERT" if idempotent else "POST"

        for result in results:
            idempotency_key = make_idempotency_key(result)
            row = {
                **result,
                "idempotency_key": idempotency_key,
            }

            for attempt in range(1, attempts + 1):
                audit.append(
                    {
                        **row,
                        "attempt": attempt,
                        "operation": operation,
                    }
                )

                if idempotent:
                    upsert_sink[idempotency_key] = dict(row)
                else:
                    append_sink.append(dict(row))

        materialized = (
            list(upsert_sink.values()) if idempotent else append_sink
        )
        return materialized, audit

    return make_idempotency_key, simulate_sink_retries


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos e idempotencia

    El sink se modela con dos contratos:

    | Modo | Operación | Consecuencia de dos intentos |
    |---|---|---|
    | `POST` append-only | `append(row)` | dos filas visibles |
    | `UPSERT` idempotente | `sink[key] = row` | una entidad lógica |

    La clave `merchant_id|window_start` representa el mismo efecto lógico para
    reintentos y para revisiones acumulativas de una ventana. Así, una nueva
    revisión reemplaza el total anterior en lugar de crear otra entidad.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 5. Verificación

    Ejecutar desde el repositorio:

    ```bash
    uv sync --frozen
    uv run pytest
    uv run ruff check notebook.py
    uv run marimo check --strict notebook.py
    ```

    Además de la suite provista, se incluye una prueba con `TestStream` para
    demostrar que un evento con timestamp perteneciente a una ventana cerrada
    puede llegar después de que el watermark cruza el final de esa ventana y
    aun producir una revisión mientras permanezca dentro de `allowed_lateness`.
    """)
    return


if __name__ == "__main__":
    app.run()
