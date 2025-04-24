-- активные работы
WITH active_works_hk AS (
	SELECT
		lnk.hk_dv_hub_fact_work AS hk_dv_hub_fact_work
	FROM(
		SELECT
			hk_dv_lnk_fact_journal_fact_work,
			ROW_NUMBER() OVER(PARTITION BY hk_dv_lnk_fact_journal_fact_work ORDER BY start_date DESC) AS rn
		FROM dv_esat_fact_journal_fact_work es) es
	JOIN dv_lnk_fact_journal_fact_work lnk using(hk_dv_lnk_fact_journal_fact_work)
	JOIN (
		SELECT DISTINCT hk_dv_hub_fact_journal
		FROM public.dv_sat_fact_journal fj
		WHERE fj."проведен" IS TRUE AND fj.пометка_удаления IS FALSE
	) fj using(hk_dv_hub_fact_journal)
)


SELECT
    hk_dv_hub_fact_work,
    recsource,
    loadts,
    hdiff_dv_sat_fact_work,
    идентификатор,
    объем_работы,
    примечание,
    кв,
    пометка_удаления,
    тип_spider,
    структура_работ_value,
    структура_работ_codе,
    структура_работ_name,
    видработ_value,
    видработ_codе,
    видработ_name
FROM 
	(SELECT 
		fw.*,
	    ROW_NUMBER() OVER(PARTITION BY hk_dv_hub_fact_work ORDER BY loadts DESC) AS rn
	FROM dv_sat_fact_work fw) fw
JOIN active_works_hk USING(hk_dv_hub_fact_work)
WHERE rn = 1