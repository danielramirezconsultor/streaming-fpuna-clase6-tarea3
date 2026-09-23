# Evidencia de ejecución — Tarea 3

Este archivo está preparado para registrar **salidas reales** antes de la entrega. No completar con resultados inferidos.

## 1. Suite de pruebas

Ejecutar:

```bash
uv run pytest
```

Pegar debajo la salida completa o, como mínimo, el resumen final:

```text
[PENDIENTE DE EJECUCIÓN]
```

## 2. Ruff

Ejecutar:

```bash
uv run ruff check notebook.py
```

Resultado:

```text
[PENDIENTE DE EJECUCIÓN]
```

## 3. Marimo strict check

Ejecutar:

```bash
uv run marimo check --strict notebook.py
```

Resultado:

```text
[PENDIENTE DE EJECUCIÓN]
```

## 4. Evidencia temporal con TestStream

La prueba `tests/test_additional.py` debe demostrar que:

1. un elemento inicial pertenece a la ventana `[0, 60)`;
2. el watermark cruza 60 y produce el resultado on-time;
3. después llega otro elemento con timestamp 50;
4. el elemento todavía está dentro de `allowed_lateness=120`;
5. el pane acumulativo late contiene el total revisado;
6. `PaneInfoParam` hace visibles `timing`, `index`, `is_first` e `is_last`.

Registrar aquí cualquier salida adicional o captura de la ejecución en Marimo si se utiliza como evidencia visual.
