{{
    config(
        materialized='table',
        tags=['жуфвр_1с']
    )
}}



WITH active_lnk AS (
	SELECT hk_dv_lnk_fact_work_tech
	FROM (
		SELECT 
			hk_dv_lnk_fact_work_tech,
			end_date,
			ROW_NUMBER() OVER(PARTITION BY hk_dv_lnk_fact_work_tech ORDER BY loadts desc) rn
		FROM {{ ref('dv_esat_fact_work_tech') }}) t
	WHERE rn = 1 AND end_date IS NULL)
	

SELECT 
	lnk.hk_dv_lnk_fact_work_tech,
	lnk.hk_dv_hub_fact_work,
	lnk.hk_dv_hub_tech,
	
	sat.госномер_техника_не_найдена,
	sat.количество,
	sat.часы,
	sat.ресурс_name,
	sat.аналитика_name,
	sat.контрагент_name

FROM 
	(SELECT
		hk_dv_lnk_fact_work_tech,
		
		госномер_техника_не_найдена,
		количество,
		часы,
		ресурс_name,
		аналитика_name,
		контрагент_name,
		
		ROW_NUMBER() OVER(PARTITION BY hk_dv_lnk_fact_work_tech ORDER BY loadts desc) rn
		
	FROM {{ ref('dv_sat_fact_tech') }} sat) sat
	
JOIN active_lnk act_ln USING(hk_dv_lnk_fact_work_tech)
JOIN {{ ref('dv_lnk_fact_work_tech') }} lnk USING(hk_dv_lnk_fact_work_tech)
WHERE rn = 1

