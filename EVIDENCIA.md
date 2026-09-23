# Evidencia de ejecución — Tarea 3

## Entorno

La validación se realizó en GitHub Codespaces utilizando el entorno reproducible definido por el proyecto.

Versiones principales observadas durante la ejecución:

- Python 3.12.3
- Apache Beam 2.74.0
- pytest 8.4.2
- Marimo 0.23.15
- Ruff 0.16.0

## Instalación reproducible

Se ejecutó:

```bash
uv sync --frozen
```

Resultado: sincronización completada correctamente utilizando el `uv.lock` provisto por el proyecto.

## Suite de pruebas

Se ejecutó:

```bash
uv run pytest
```

Resultado final:

```text
collected 16 items

tests/test_additional.py ...       [ 18%]
tests/test_assignment.py ............. [100%]

16 passed in 14.07s
```

Las 16 pruebas incluyen la suite provista por la cátedra y tres pruebas complementarias en `tests/test_additional.py`.

Entre los comportamientos verificados se encuentran:

- parsing de timestamps;
- ventanas fijas;
- uso de `event_time`;
- eventos fuera de orden;
- deduplicación;
- aislamiento de estado por comercio;
- datos tardíos aceptados;
- eventos fuera de la tolerancia;
- panes acumulativos;
- metadatos de pane mediante `TestStream`;
- expiración del estado mediante timer;
- reintentos del sink;
- idempotencia mediante `UPSERT`.

## Validación con Ruff

Se ejecutó:

```bash
uv run ruff check notebook.py
```

Resultado:

```text
All checks passed!
```

## Validación de Marimo

Se ejecutó:

```bash
uv run marimo check --strict notebook.py
```

El comando finalizó sin errores ni advertencias.

## Resultado

- `pytest`: aprobado — 16 pruebas.
- `ruff`: aprobado.
- `marimo check --strict`: aprobado.