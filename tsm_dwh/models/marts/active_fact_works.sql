{{
    config(
        materialized='table',
        tags=['жуфвр_1с']
    )
}}


WITH active_journal AS (
	SELECT *
	FROM
		(SELECT 
			hk_dv_hub_fact_journal,
			
			версия_жуфвр_name,
			дата,
			территория_name,
			подразделение_name,
			смена_name,
			ответственный_name,
			направление_деятельности_name,
			
			row_number() OVER(PARTITION BY hk_dv_hub_fact_journal ORDER BY loadts DESC) rn
			
		FROM {{ ref('dv_sat_fact_journal') }} j
		WHERE проведен is TRUE AND пометка_удаления is FALSE) t
	WHERE rn = 1)


, active_works AS (
SELECT *
	FROM (
	SELECT
		hk_dv_hub_fact_work,
		
		идентификатор,
		кв,
		тип_spider,
		структура_работ_name,
		видработ_name,
		объем_работы,
		
		row_number() OVER(PARTITION BY hk_dv_hub_fact_work ORDER BY loadts DESC) rn
		
	FROM {{ ref('dv_sat_fact_work') }}
WHERE пометка_удаления IS FALSE) t 
WHERE rn = 1)


SELECT *
from active_works
