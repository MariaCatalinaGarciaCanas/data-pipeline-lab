{#
  Test generico: la combinacion de columnas es unica (la clave compuesta no se repite).
  Devuelve las combinaciones repetidas; si devuelve alguna fila, el test falla.
  Uso:  data_tests: [{unique_combination: {combination_columns: [col_a, col_b]}}]
#}
{% test unique_combination(model, combination_columns) %}

select
    {{ combination_columns | join(', ') }},
    count(*) as repeated_rows
from {{ model }}
group by {{ combination_columns | join(', ') }}
having count(*) > 1

{% endtest %}
