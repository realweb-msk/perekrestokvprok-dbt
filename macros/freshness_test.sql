{% test freshness(model, column_name) %}
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
        WHERE max_date < DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    )

    SELECT *    
    FROM mistakes

{% else %}
    --для разработки
    SELECT
        '{{ model }}' AS max_date
    FROM {{ model }} 
    LIMIT 1
    -- select x
    -- from {{ model }}, UNNEST([1]) as x
    -- where x < 1
{% endif %}

{% endtest %}