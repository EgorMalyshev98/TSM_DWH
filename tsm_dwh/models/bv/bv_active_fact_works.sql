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
		w.hk_dv_hub_fact_work,
		
		w.идентификатор,
		w.кв,
		w.тип_spider,
		w.структура_работ_name,
		w.видработ_name,
		w.объем_работы,
		
		row_number() OVER(PARTITION BY hk_dv_hub_fact_work ORDER BY loadts DESC) rn
		
	FROM {{ ref('dv_sat_fact_work') }} w
WHERE пометка_удаления IS FALSE) t
WHERE t.rn = 1)



SELECT
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
	w.объем_работы
	
FROM active_works w
JOIN {{ ref("dv_lnk_fact_journal_fact_work") }} lnk USING(hk_dv_hub_fact_work)
JOIN active_journal j USING(hk_dv_hub_fact_journal)

