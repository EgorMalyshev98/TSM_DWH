{{
    config(
        materialized='table'
    )
}}



SELECT
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
	
	t.трудоемкость

FROM bv_active_fact_works w
LEFT JOIN (
	SELECT 
		hk_dv_hub_fact_work,
		sum(часы) AS трудоемкость
	FROM bv_active_fact_tech t
	GROUP BY hk_dv_hub_fact_work
) t USING(hk_dv_hub_fact_work)