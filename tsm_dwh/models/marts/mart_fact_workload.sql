{{
    config(
        materialized='table'
    )
}}


WITH active_lnk_fact_work_tech AS (
	SELECT hk_dv_lnk_fact_work_tech
	FROM (
		SELECT 
			hk_dv_lnk_fact_work_tech,
			end_date,
			ROW_NUMBER() OVER(PARTITION BY hk_dv_lnk_fact_work_tech ORDER BY loadts desc) rn
		FROM {{ ref('dv_esat_fact_work_tech') }}) t
	WHERE rn = 1 AND end_date IS NULL)
	
, active_tech AS MATERIALIZED(
SELECT
	lnk.hk_dv_lnk_fact_work_tech,
	lnk.hk_dv_hub_fact_work,
	lnk.hk_dv_hub_tech,

	hub.bk_ресурс_uuid,
	
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
	
JOIN active_lnk_fact_work_tech alnk USING(hk_dv_lnk_fact_work_tech)
JOIN {{ ref('dv_lnk_fact_work_tech') }} lnk USING(hk_dv_lnk_fact_work_tech)
JOIN {{ ref('dv_hub_tech') }} hub USING(hk_dv_hub_tech)

WHERE rn = 1 AND bk_ресурс_uuid != '-1'
)


, active_journal AS (
	SELECT *
	FROM
		(SELECT 
			hk_dv_hub_fact_journal,
			
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
			
			row_number() OVER(PARTITION BY hk_dv_hub_fact_journal ORDER BY loadts DESC) rn
			
		FROM {{ ref('dv_sat_fact_journal') }} j
		WHERE проведен is TRUE AND пометка_удаления is FALSE
		) t
	WHERE rn = 1)


, sat_work AS (
SELECT *
	FROM (
	SELECT
		w.hk_dv_hub_fact_work,
		
		w.идентификатор,
		w.кв,
		w.тип_spider,
		w.структура_работ_name,
		w.видработ_name,
		w.объем_работы,
		w.пометка_удаления,
		
		row_number() OVER(PARTITION BY hk_dv_hub_fact_work ORDER BY loadts DESC) rn
		
	FROM {{ ref('dv_sat_fact_work') }} w
WHERE пометка_удаления IS FALSE
) t
WHERE t.rn = 1)

, active_fact_works AS MATERIALIZED
(SELECT
	w.hk_dv_hub_fact_work,
	j.hk_dv_hub_fact_journal,
	
	j.версия_жуфвр_name,
	j.дата,
	j.территория_name,
	j.подразделение_name,
	j.смена_name,
	j.ответственный_name,
	j.направление_деятельности_name,
	
	w.идентификатор,
	w.кв,
	w.тип_spider,
	w.структура_работ_name,
	w.видработ_name,
	w.объем_работы,

    r.наименование as объект,
  	
    (nw.нормируемая IS NOT NULL) AS нормируемая,
	
	w.пометка_удаления AS работа_пометка_удаления,
	j.проведен,
	j.пометка_удаления AS журнал_пометка_удаления
	
FROM sat_work w
JOIN {{ ref('dv_lnk_fact_journal_fact_work') }} lnk USING(hk_dv_hub_fact_work)
JOIN active_journal j USING(hk_dv_hub_fact_journal)
JOIN {{ source('public', 'ref_object_names') }} r USING(территория_value)
LEFT JOIN (
	SELECT DISTINCT 
		тип_spider,
		TRUE::boolean AS нормируемая
	FROM dv_msat_norm_workload
    JOIN sat_work USING(hk_dv_hub_fact_work)
) nw USING(тип_spider)

WHERE 
	j.проведен IS TRUE
	AND j.пометка_удаления IS FALSE
	AND w.пометка_удаления IS FALSE
    AND r.is_active is true
)



SELECT
    encode(t.hk_dv_hub_fact_work, 'hex') as hk_dv_hub_fact_work,
    encode(t.hk_dv_hub_tech, 'hex') as hk_dv_hub_tech,
	t.bk_ресурс_uuid,
	
	t.госномер_техника_не_найдена,
	t.количество,
	t.часы,
	t.ресурс_name,
	t.аналитика_name,
	t.контрагент_name,

    w.версия_жуфвр_name,
	w.дата,
	w.территория_name,
	w.подразделение_name,
	w.смена_name,
	w.ответственный_name,
	w.направление_деятельности_name,
	w.идентификатор,
	w.кв,
	w.тип_spider,
	w.структура_работ_name,
	w.видработ_name,
	w.объем_работы,
    w.объект,
    w.нормируемая
    
FROM active_tech t
JOIN active_fact_works w USING(hk_dv_hub_fact_work)

