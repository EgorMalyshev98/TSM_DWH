{{
    config(
        materialized='table'
    )
}}


with active_journal AS (
	SELECT 
        t.hk_dv_hub_fact_journal,
        t.версия_жуфвр_name,
        t.дата,
        t.территория_name,
        t.территория_value,
        t.подразделение_name,
        t.смена_name,
        t.год,
		t.направление_деятельности_name,
        
        last_name || ' ' || first_name || '. ' || second_name || '.' as ответственный,

        r.наименование AS объект
	FROM
		(SELECT 
			encode(hk_dv_hub_fact_journal, 'hex') as hk_dv_hub_fact_journal,
			
			версия_жуфвр_name,
			дата,
			территория_name,
			TRIM(территория_value::varchar) as территория_value,
			подразделение_name,
			смена_name,
			ответственный_name,
			направление_деятельности_name,
			проведен,
			пометка_удаления,

			extract(year from дата) as год,

			row_number() OVER(PARTITION BY hk_dv_hub_fact_journal ORDER BY loadts DESC) rn,

            (string_to_array(ответственный_name, ' '))[1] AS last_name,
            substring((string_to_array(ответственный_name, ' '))[2], 1, 1) AS first_name,
            substring((string_to_array(ответственный_name, ' '))[3], 1, 1) AS second_name
			
		FROM {{ ref('dv_sat_fact_journal') }} j
		WHERE проведен is TRUE AND пометка_удаления is FALSE
		) t
    JOIN {{ source('public', 'ref_object_names') }} r USING(территория_value)
	WHERE rn = 1
    and r.is_active is true
    
    )


SELECT * FROM active_journal