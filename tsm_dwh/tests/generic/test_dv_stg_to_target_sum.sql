-- проверка кол-ва строк в между слоями stage и data vault
-- model
-- column_name
-- source_model
-- hk_name
-- hdiff_name


{% test test_dv_stg_to_target_sum(model, column_name, stg_model, hk_name, hdiff_name, sum_col) %}


WITH
    stg AS (
        SELECT 
            sum({{ sum_col }})
        FROM (
            SELECT DISTINCT ON ({{ hk_name }}, {{ hdiff_name }})
                {{ hk_name }},
                {{ hdiff_name }},
                {{ sum_col }}
            FROM {{ stg_model }}
            where 
                loadts >= (SELECT min(loadts) FROM {{ model }} )
                AND
                loadts <= (SELECT max(loadts) FROM {{ model }} )
        ) t
    ),
    target AS (
        SELECT 
            sum({{ sum_col }})
        FROM (
            SELECT DISTINCT ON ({{ hk_name }}, {{ hdiff_name }})
                {{ hk_name }},
                {{ hdiff_name }},
                {{ sum_col }}
            FROM {{ model }}
        ) t
    )

SELECT
    stg.sum, target.sum
FROM stg, target

WHERE abs((stg.sum - target.sum)) > 10


{% endtest %}

