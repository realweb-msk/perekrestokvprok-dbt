{% test freshness(model, column_name, days = 1) %}
{% set dt = modules.datetime.date.today() %}
{% if modules.datetime.date.isoweekday(dt) < 6 %}
    -- фейлится если в рабочие дни данные за вчера не обновлены
    WITH source AS (
    SELECT
        DATE(MAX({{ column_name }})) AS max_date
    FROM {{ model }}
    ),

    mistakes AS (
        SELECT max_date
        FROM source
        WHERE max_date < DATE_SUB(CURRENT_DATE(), INTERVAL {{ days }} DAY)
    )

    SELECT *    
    FROM mistakes

{% else %}

    SELECT x
    FROM {{ model }}, UNNEST([1]) AS x
    WHERE x < 1

{% endif %}

{% endtest %}