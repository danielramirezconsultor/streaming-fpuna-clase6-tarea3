# Tarea 3 — Beam avanzado

Solución de la Tarea 3 de **Streaming de datos y sus aplicaciones**.

El proyecto implementa un pipeline de pagos con Apache Beam que produce totales confirmados por comercio y minuto, preservando la semántica de tiempo de evento ante desorden, duplicados, datos tardíos y reintentos del sink.

## Objetivo

La implementación:

- usa `event_time`, no el tiempo de llegada;
- aplica ventanas fijas de 60 segundos;
- tolera hasta 120 segundos de atraso;
- descarta estados distintos de `CONFIRMED`;
- deduplica `event_id` dentro de cada comercio;
- utiliza panes acumulativos;
- conserva metadatos de ventana;
- permite observar metadatos de pane mediante `PaneInfo`;
- materializa la salida con la clave idempotente `merchant_id|window_start`.

## Decisiones de implementación

### Tiempo de evento y ventanas

Cada pago se convierte en un `TimestampedValue` usando `event_time`.

La agregación se realiza en ventanas fijas de 60 segundos. De esta manera, un evento fuera de orden conserva la ventana correspondiente al momento en que ocurrió y no a su momento de llegada.

### Estado y deduplicación

`DeduplicatePayments` recibe elementos previamente claveados por `merchant_id`.

Un `SetStateSpec` mantiene los `event_id` observados dentro de cada clave/ventana. Si el mismo identificador vuelve a aparecer para el mismo comercio, no se vuelve a emitir.

Un timer en tiempo de evento limpia el estado después de:

```text
window.end + allowed_lateness
```

Esto evita que el estado de deduplicación crezca indefinidamente y permite acotar el uso de memoria y el tamaño de los checkpoints.

### Triggers y datos tardíos

La política temporal definida en `build_trigger_policy()` utiliza:

- `AfterWatermark` para la emisión on-time;
- `AfterProcessingTime(10)` para una estimación early;
- `AfterCount(1)` para revisiones late;
- `allowed_lateness=120`;
- `AccumulationMode.ACCUMULATING`.

Los panes acumulativos representan en cada revisión el total completo de la ventana.

La salida principal mantiene el contrato:

```text
merchant_id, window_start, window_end, total
```

Para hacer observable también el comportamiento de los panes, se incluye una prueba adicional con `TestStream` y `PaneInfoParam`.

### Idempotencia

La clave idempotente utilizada es:

```text
merchant_id|window_start
```

Con una operación tipo `UPSERT`, dos intentos de escribir el mismo resultado convergen sobre una sola entidad lógica.

La función `simulate_sink_retries()` permite contrastar este comportamiento con un sink `POST` append-only, en el cual cada reintento genera una nueva fila.

## Dataset provisto

Con la configuración por defecto, `data/payments.jsonl` contiene 9 registros.

El oráculo determinista:

- acepta 5 eventos;
- produce 4 totales lógicos;
- identifica duplicados;
- identifica revisiones tardías aceptadas;
- audita eventos que superan los 120 segundos de lateness.

`data/payments.jsonl` no fue modificado.

## Ejecutar con uv

Sincronizar las dependencias:

```bash
uv sync --frozen
```

Ejecutar las pruebas:

```bash
uv run pytest
```

Validar estilo y estructura:

```bash
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
```

Abrir el notebook en Marimo:

```bash
uv run marimo edit notebook.py
```

## Ejecutar con Docker

Desde la raíz del repositorio:

```bash
docker compose up --build notebook
```

Abrir:

```text
http://localhost:2718
```

Dentro del contenedor también puede ejecutarse:

```bash
docker compose exec notebook uv run pytest
```

## Pruebas

Se mantienen sin modificaciones las pruebas provistas por la cátedra.

Además, se agrega `tests/test_additional.py` para complementar la suite con una prueba temporal basada en `TestStream`, incluyendo una revisión tardía y observación de `PaneInfo`.

Los casos evaluados incluyen:

- parsing temporal;
- asignación de ventanas;
- eventos fuera de orden;
- duplicados;
- aislamiento del estado por comercio;
- eventos tardíos aceptados;
- eventos fuera de la tolerancia;
- expiración del estado mediante timer;
- comportamiento del sink ante reintentos;
- idempotencia mediante `UPSERT`.

## Trade-offs

**Event time en lugar de arrival time.**  
Preserva el significado temporal del dominio y hace que un replay mantenga el mismo agrupamiento de los eventos.

**Estado por comercio y ventana.**  
Evita una deduplicación global accidental, pero requiere una política explícita de expiración.

**Panes acumulativos.**  
Simplifican el consumo downstream porque cada revisión representa el total completo de la ventana, a cambio de volver a emitir información ya observada.

**UPSERT idempotente.**  
Evita duplicar efectos ante reintentos, pero requiere una clave estable y un destino que permita reemplazar o actualizar la misma entidad lógica.

## Evidencia de ejecución

Antes de la entrega final se registrarán los resultados reales de:

```bash
uv run pytest
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
```

La evidencia se conserva en `EVIDENCIA.md`.
