-- проверка кол-ва строк в между слоями stage и data vault
-- model
-- column_name
-- source_model
-- hk_name
-- hdiff_name


{% test test_dv_stg_to_target_count(model, column_name, stg_model, hk_name, hdiff_name) %}


WITH
	stg_count AS (
	    SELECT COUNT(DISTINCT ({{ hk_name }}, {{ hdiff_name }})) AS src_count
	    FROM {{ stg_model }}
		WHERE 
			loadts >= (SELECT min(loadts) FROM {{ model }})
			AND
			loadts <= (SELECT max(loadts) FROM {{ model }})
	),
	target_count AS (
	    SELECT COUNT(DISTINCT ({{ hk_name }}, {{ hdiff_name }})) AS target_count
	    FROM {{ model }}
	)

SELECT
	    stg_count.src_count,
	    target_count.target_count
FROM stg_count, target_count
WHERE src_count != target_count

{% endtest %}

