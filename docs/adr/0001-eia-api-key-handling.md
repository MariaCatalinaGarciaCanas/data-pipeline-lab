# ADR 0001: Manejo de la API key de EIA en el cliente HTTP

- **Estado:** Propuesto (Fase 1, PR 2/5). Pasa a "Aceptado" al fusionar la PR.
- **Fecha:** 2026-11-16
- **Ambito:** `src/pipeline_lab/eia/` (cliente), `src/pipeline_lab/logging_config.py`.

> **Resumen honesto:** este diseno **reduce** de forma medible las vias por las que la API key
> puede acabar en un log, una excepcion o un traceback. **No garantiza** que la clave este
> "completamente protegida": hay vias que el codigo no puede cerrar (ver "Limitaciones").

## Contexto

La API v2 de EIA solo acepta la clave como parametro `api_key` **de la URL**; la documentacion
oficial indica que la ignora si va en una cabecera HTTP. Por tanto la URL de cada peticion
contiene la clave, y `httpx` incluye esa URL en varios sitios:

- el mensaje de `HTTPStatusError` (`raise_for_status()`),
- el atributo `request.url` de cualquier excepcion de `httpx` (`exc.request`),
- `repr(request)` y `str(response.url)`,
- los logs de `httpx`/`httpcore` (a nivel INFO, una linea por peticion con la URL completa),
- los hooks de `httpx` (`event_hooks`) y cualquier instrumentacion que se apoye en ellos.

Ademas, un servidor (o un intermediario) puede devolver la URL o la clave dentro del cuerpo de un
mensaje de error, y las herramientas que vuelcan variables locales (`pytest --showlocals`,
`traceback` con `capture_locals`, herramientas de observabilidad) exponen lo que haya en los
frames del traceback.

### Medicion previa (servidor local, clave ficticia, httpx 0.28.1)

Se compararon tres disenos frente a 7 fallos (conexion, timeout, 503, 403 y 400 con eco de la
clave, 200 con HTML con eco, redireccion 302):

| Diseno | `str`/`repr`/traceback | con `showlocals` | `__context__` | Objetos alcanzables con la clave |
|---|---|---|---|---|
| A. `raise_for_status()` y propagar | fuga en los casos HTTP | fuga en los 7 | | si |
| B. `raise ... from None` **dentro** del `except` | limpio | limpio | **sigue enlazado** | **si (1 a 5 rutas)** |
| C. datos saneados y `raise` **fuera** del `except` | limpio | limpio | `None` | **ninguna en los 7** |

La leccion principal: el diseno B **parece** seguro (un test que solo mire `str(exc)`,
`repr(exc)` y el traceback lo daria por bueno) pero la excepcion original de `httpx`, con su URL,
sigue accesible desde `exc.__context__`. `from None` solo oculta el contexto al *imprimir*.

## Decision

Se adopta el **diseno C**:

1. **`_transport()` es el unico sitio donde existen objetos de `httpx`.** Captura cualquier fallo
   de la llamada y devuelve **datos** (`_Reply`/`_Failure`), no excepciones. Los objetos de
   `httpx` se eliminan antes de interpretar la respuesta.
2. **`_execute()` decide los reintentos y lanza las excepciones propias, sin ningun `except`.**
   Las excepciones se lanzan fuera de todo bloque de captura: `__context__` y `__cause__` son
   `None` y ningun objeto de `httpx` esta enlazado a la excepcion ni a las variables locales de
   sus frames.
3. **Las excepciones propias solo contienen texto y numeros saneados** (`exceptions.py`).
4. **El texto del servidor se trata como no confiable** (`_sanitize.py`): se eliminan caracteres
   de control, se descarta *todo* el mensaje si contiene la clave o un fragmento literal de 8 o
   mas caracteres (tambien codificada en URL), se enmascara cualquier coincidencia exacta restante
   y se trunca a 200 caracteres. El codigo de error de EIA se descarta si no tiene forma de
   codigo (o si es demasiado largo).
5. **No se llama a `raise_for_status()`**; el estado HTTP se interpreta manualmente.
6. **`follow_redirects=False`**: una redireccion a otro host llevaria la clave en la URL. Una
   respuesta 3xx es un error (`EIAResponseError`).
7. **`base_url` debe ser `https`**: la clave viaja en la URL.
8. **La clave se revela (`Secret.reveal()`) solo en el argumento de la llamada HTTP**, sin guardarla
   en una variable local con nombre. `EIAClient.__repr__` no la incluye.
9. **Logs:** el cliente solo registra evento, numero de intento, tipo de fallo, retardo y estado,
   nunca URL, parametros ni cuerpos. Su constructor llama a `silence_http_loggers()` (no depende
   de que el programa haya configurado el logging) y `MaskingFormatter`/`mask_secrets()` enmascaran
   la clave en el texto final como segunda barrera.
10. **`Secret` no se modifica.** Sigue siendo una proteccion contra *accidentes*, no contra acceso
    deliberado.

Un `Retry-After` mayor que el limite configurado (30 s por defecto) **no se reintenta**: reintentar
antes de lo que pide el servidor es la via para que EIA suspenda la clave.

## Evidencia (como se comprobo)

Estas pruebas **respaldan** el diseno; no lo demuestran formalmente.

- **Matriz de 16 escenarios hostiles** (`tests/unit/eia/test_client_security.py`): el servidor o
  `httpx` repiten la clave literal, codificada, parcial, en el codigo de error, dentro de la URL
  real de la peticion, en el `Location` de una redireccion, etc. Para cada uno se comprueba que la
  clave (o un fragmento de 8+ caracteres) no aparece en `str`, `repr`, `args`, `vars()`, el
  traceback completo, el traceback con variables locales (modulo `traceback` y `pytest
  --showlocals`), ni en el pickle; que `__context__` y `__cause__` son `None`; que ningun objeto de
  `httpx` esta en las variables locales de los frames; y que el grafo de objetos alcanzable desde
  la excepcion no contiene la clave (tratando `Secret` como opaco).
- **Los detectores se prueban a si mismos:** se fabrican excepciones que filtran la clave de seis
  maneras distintas y se comprueba que los detectores las ven (y que no dan falsos positivos).
- **Los escenarios son hostiles de verdad:** una prueba comprueba que cada escenario entrega la
  clave al cliente, para que el resultado no sea verde en vacio.
- **Logs:** con los loggers de `httpx` forzados a DEBUG y el `MaskingFormatter` activo, la clave no
  aparece; sin el formatter, `httpx` SI la escribe (prueba que lo demuestra).
- **Pruebas de mutacion:** se rompe el codigo a proposito en copias del proyecto (lanzar dentro del
  `except`, conservar la respuesta en una variable, quitar el saneado, etc.) y se comprueba que algun
  test falla. Se aplicaron 41 mutaciones: 16 heredadas de la PR 1 (adaptadas al refactor de
  `logging_config.py`) y 25 del cliente. Resultado real, en dos ejecuciones:
  - **Primera ejecucion: 40 detectadas y 1 superviviente (N5, "codigo de error sin sanear").** Era un
    hueco real de las pruebas, no del codigo: la clave ficticia principal contiene `/`, `+` y `=`, de
    modo que el patron de forma de un codigo de error ya la descartaba y la llamada a
    `sanitize_untrusted_text` quedaba sin ejercitar. Con una clave alfanumerica (que SI cumple ese
    patron) el saneado es la unica barrera.
  - **Se cerro con** `test_alphanumeric_key_echoed_as_error_code_is_discarded`, que pasa con el codigo
    actual y falla con el mutante N5.
  - **Segunda ejecucion (tras ese test): 41 de 41 detectadas.**

  Esto indica que las pruebas detectan esos 41 cambios concretos. **No demuestra que no queden otros
  huecos:** las mutaciones las eligio el autor y no son exhaustivas.
- **Control con transporte real** (fuera de la suite, servidor local por TCP): `respx` sustituye
  el transporte, asi que el pool de conexiones real nunca se ejercita en las pruebas del
  proyecto. Con conexiones reales, 503, 403 con eco, timeout y conexion rechazada dieron texto limpio
  y ninguna ruta a la clave en el grafo.

## Limitaciones (lo que NO esta protegido)

1. **Durante la llamada HTTP.** `httpx` tiene la URL con la clave en sus frames internos y la
   entrega a los *hooks*. Una prueba documenta que un hook de `httpx` ve la URL completa. Cualquier
   instrumentacion que se enganche a `httpx` (por ejemplo OpenTelemetry o integraciones de
   observabilidad) podria registrarla. **No se probo el comportamiento de ninguna herramienta
   concreta.** Mitigacion: no activarlas sobre este cliente, o configurar su redaccion de URLs.
2. **Red y servidor.** EIA obliga a poner la clave en la URL: proxies con inspeccion TLS, los
   registros del servidor de EIA y cualquier intermediario que registre URLs la ven. Es inherente
   a la API y no se puede evitar desde este codigo.
3. **Memoria del proceso.** La clave existe como cadena de Python (inmutable, no se puede
   borrar). Un volcado de memoria, un depurador detenido durante la llamada, o codigo que lea
   `client._api_key._value` la encuentran. `Secret` previene accidentes, no acceso deliberado. Una
   prueba lo deja documentado.
4. **Eco transformado.** El saneado detecta la clave literal, codificada en URL y fragmentos
   literales de 8+ caracteres. **No detecta** la clave transformada (base64, invertida, partida en
   trozos cortos, etc.). Hay una prueba que documenta este limite.
5. **Otros datos sensibles en mensajes de terceros.** Solo se garantiza el tratamiento de la API
   key. El mensaje de EIA podria contener otra informacion sensible; se reduce el riesgo con el
   truncado a 200 caracteres y la eliminacion de caracteres de control, y el texto **no se escribe
   en los logs**, solo en la excepcion.
6. **Filas de datos.** `EIAPage.rows` contiene el JSON de la respuesta tal cual; solo se sanean
   los textos de error y de avisos (`warnings`), no los datos.
7. **El grafo de objetos desde `self`.** Cualquier frame que contenga el cliente permite llegar a
   `Secret._value`. Es inherente a que el cliente guarde la clave.
8. **Alcance de las pruebas.** Se probo con `httpx` 0.28.1 (limitado a `<0.29`), en Windows, con una
   clave ficticia. Otras versiones, plataformas o formas de clave pueden comportarse distinto.
9. **No se probo con la clave real** ni se ha leido el archivo `.env` en ninguna prueba.

## Reglas para quien modifique este codigo

- No usar `raise_for_status()` ni dejar escapar excepciones de `httpx`.
- No lanzar excepciones dentro de un `except` que haya capturado una excepcion de `httpx`.
- No guardar `Request`, `Response`, `URL` ni la clave en variables locales fuera de `_transport()`.
- No registrar URLs, parametros de consulta ni cuerpos de respuesta.
- No activar hooks ni instrumentacion sobre `httpx` en este cliente sin tratar la URL.
- No aflojar un test de seguridad para que pase: si falla, hay una fuga o el test esta mal.

## Alternativas descartadas

- **Enviar la clave en una cabecera:** EIA la ignora.
- **`raise ... from None` dentro del `except` (diseno B):** deja `__context__` accesible con la URL.
- **Capturar solo `httpx.HTTPError`:** `InvalidURL`, `StreamError` y otras excepciones de `httpx`
  tambien pueden llevar la URL; se captura cualquier excepcion **de la llamada**.
- **`httpx.Auth` para inyectar la clave tarde:** la URL final de la peticion enviada sigue
  conteniendo la clave, asi que no elimina la exposicion.
- **Ofuscar `Secret._value`:** daria una falsa sensacion de seguridad.

## Referencias

- Documentacion de la API v2 de EIA: <https://www.eia.gov/opendata/documentation.php>
- `src/pipeline_lab/eia/client.py` (diseno C), `src/pipeline_lab/eia/_sanitize.py`
- `tests/unit/eia/test_client_security.py`
