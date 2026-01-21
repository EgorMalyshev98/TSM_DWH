{{
    config(
        materialized='table'
    )
}}

-- нормативные работы с трудоемкостью по ресурсам

WITH active_norm_wld AS (

SELECT
	hk_dv_hub_fact_work,
	аналитика_value,
	наемный_ресурс,
	ресурс_spider_name,
	ресурс_spider_codе,
	ключевой_ресурс_value,
	ключевой_ресурс_name,
	несколько_ключевых_ресурсов,

	sum(трудоемкость_нормативная) AS трудоемкость_нормативная,
	sum(трудоемкость_фактическая) AS трудоемкость_фактическая
	
FROM (
SELECT
	hk_dv_hub_fact_work,
	-- для join с bk LOWER(TRIM(COALESCE(аналитика_value::varchar, '-1'))) AS аналитика_value,
	аналитика_value,
	
	трудоемкость_нормативная,
	трудоемкость_фактическая,
	наемный_ресурс,
	ресурс_spider_name,
	ресурс_spider_codе,
	ключевой_ресурс_value,
	ключевой_ресурс_name,
	несколько_ключевых_ресурсов,
	
	RANK() OVER(PARTITION BY hk_dv_hub_fact_work ORDER BY loadts DESC) rk
	
FROM {{ ref('dv_msat_norm_workload') }}
	) t 
WHERE rk = 1
GROUP BY 
	hk_dv_hub_fact_work,
	аналитика_value,
	наемный_ресурс,
	ресурс_spider_name,
	ресурс_spider_codе,
	ключевой_ресурс_value,
	ключевой_ресурс_name,
	несколько_ключевых_ресурсов
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
			CASE
				WHEN версия_жуфвр_name = 'Версия 01.01.2022'
				THEN 3
				WHEN версия_жуфвр_name = 'Версия 01.10.2025'
				THEN 4
			END AS енрп_версия,
			
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
	j.енрп_версия,
	
	w.идентификатор,
	w.кв,
	w.тип_spider,
	w.структура_работ_name,
	w.видработ_name,
	w.объем_работы,

    r.наименование as объект,
	
	w.пометка_удаления AS работа_пометка_удаления,
	j.проведен,
	j.пометка_удаления AS журнал_пометка_удаления
	
FROM sat_work w
JOIN {{ ref('dv_lnk_fact_journal_fact_work') }} lnk USING(hk_dv_hub_fact_work)
JOIN active_journal j USING(hk_dv_hub_fact_journal)
JOIN {{ source('public', 'ref_object_names') }} r USING(территория_value)

WHERE 
	j.проведен IS TRUE
	AND j.пометка_удаления IS FALSE
	AND w.пометка_удаления IS FALSE
    AND r.is_active is true
)


SELECT
    encode(nw.hk_dv_hub_fact_work, 'hex') as hk_dv_hub_fact_work,
	nw.аналитика_value,
	nw.наемный_ресурс,
	nw.ресурс_spider_name,
	nw.ресурс_spider_codе,
	nw.ключевой_ресурс_value,
	nw.ключевой_ресурс_name,
	nw.несколько_ключевых_ресурсов,
	nw.трудоемкость_нормативная,
	nw.трудоемкость_фактическая,

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

	a.prod,
	a.res_cnt,
	a."load",
	a.enrp_version,

-- абс % отклонения трудоемкости от нормы
	abs(
		((sum(nw.трудоемкость_фактическая) OVER(PARTITION BY hk_dv_hub_fact_work)
		-
		sum(nw.трудоемкость_нормативная) OVER(PARTITION BY hk_dv_hub_fact_work))
		/
		NULLIF(sum(nw.трудоемкость_нормативная) OVER(PARTITION BY hk_dv_hub_fact_work), 0))
		* 100
		) as абс_отклонение,

-- абс % отклонения трудоемкости от нормы только по ключевым ресурсам
	abs(
	    (
	      sum(nw.трудоемкость_фактическая)
	        FILTER (WHERE nw.ключевой_ресурс_value = nw.аналитика_value)
	        OVER (PARTITION BY hk_dv_hub_fact_work)
	      -
	      sum(nw.трудоемкость_нормативная)
	        FILTER (WHERE nw.ключевой_ресурс_value = nw.аналитика_value)
	        OVER (PARTITION BY hk_dv_hub_fact_work)
	    )
	    /
	    NULLIF(
	      sum(nw.трудоемкость_нормативная)
	        FILTER (WHERE nw.ключевой_ресурс_value = nw.аналитика_value)
	        OVER (PARTITION BY hk_dv_hub_fact_work),
	      0
	    )
	  ) * 100 AS абс_откл_ключ_ресурс,

--	расчетный объем - объем, распределенный по ключевым ресурсам в каждой работе,
--	учитывая их фактическую трудоемкость и производительность
	
    ROUND(
        объем_работы
        *
        (
            CASE
                WHEN ключевой_ресурс_value = аналитика_value
                THEN prod * трудоемкость_фактическая
            END
            /
            SUM(
                CASE
                    WHEN ключевой_ресурс_value = аналитика_value
                    THEN prod * трудоемкость_фактическая
                END
            ) OVER (PARTITION BY hk_dv_hub_fact_work)
        ),
        3
    ) AS calc_vol,
    
-- 	нормативный ключевой ресурс по работам
    CASE 
    	WHEN COUNT(*) FILTER(WHERE prod IS NOT NULL and трудоемкость_фактическая > 0) OVER(PARTITION BY hk_dv_hub_fact_work) > 0
    	THEN TRUE
    	ELSE FALSE
    END AS is_std_key_res
    
FROM active_norm_wld nw
JOIN active_fact_works w USING(hk_dv_hub_fact_work)

LEFT JOIN {{ source('public', 'ref_enrp_assign') }} a ON 
	    w.тип_spider = a.oper_type
	    AND nw.ресурс_spider_codе = a.res_code   
	    AND w.енрп_версия = a.enrp_version




