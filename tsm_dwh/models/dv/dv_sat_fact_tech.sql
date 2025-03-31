WITH active_sat AS (
SELECT DISTINCT ON (hk_dv_lnk_fact_work_tech) hk_dv_lnk_fact_work_tech, hdiff_dv_sat_fact_tech, loadts
FROM dv_sat_fact_tech
ORDER BY hk_dv_lnk_fact_work_tech, loadts DESC)

, max_load AS (
SELECT max(loadts) AS max_loadts
FROM dv_sat_fact_tech
)

, stage_cte AS (
    SELECT
        s.hk_dv_lnk_fact_work_tech,
        s.hdiff_dv_sat_fact_tech,
        s.loadts,
        s.recsource,
        lag(s.hdiff_dv_sat_fact_tech) over(PARTITION BY s.hk_dv_lnk_fact_work_tech ORDER BY s.loadts) AS lag_hdiff,
        a.loadts AS sat_loadts,
        a.hdiff_dv_sat_fact_tech AS sat_hdiff,
        примечание,
	госномер_техника_не_найдена,
	количество,
	часы,
	ресурс_value,
	ресурс_codе,
	ресурс_name,
	аналитика_codе,
	аналитика_name,
	контрагент_value,
	контрагент_codе,
	контрагент_name

    FROM
        (SELECT
            s.hk_dv_lnk_fact_work_tech,
            s.hdiff_dv_sat_fact_tech,
            s.loadts,
            s.recsource,
            примечание,
	госномер_техника_не_найдена,
	количество,
	часы,
	ресурс_value,
	ресурс_codе,
	ресурс_name,
	аналитика_codе,
	аналитика_name,
	контрагент_value,
	контрагент_codе,
	контрагент_name
        FROM stg_1c_tech s
        CROSS JOIN max_load ml
        WHERE s.loadts > ml.max_loadts) s

    LEFT JOIN active_sat a ON a.hk_dv_lnk_fact_work_tech = s.hk_dv_lnk_fact_work_tech)


INSERT INTO dv_sat_fact_tech (hk_dv_lnk_fact_work_tech, recsource, loadts, hdiff_dv_sat_fact_tech, примечание,
	госномер_техника_не_найдена,
	количество,
	часы,
	ресурс_value,
	ресурс_codе,
	ресурс_name,
	аналитика_codе,
	аналитика_name,
	контрагент_value,
	контрагент_codе,
	контрагент_name)
SELECT DISTINCT
    hk_dv_lnk_fact_work_tech,
    recsource,
    loadts,
    hdiff_dv_sat_fact_tech,
    примечание,
	госномер_техника_не_найдена,
	количество,
	часы,
	ресурс_value,
	ресурс_codе,
	ресурс_name,
	аналитика_codе,
	аналитика_name,
	контрагент_value,
	контрагент_codе,
	контрагент_name
FROM stage_cte
WHERE hdiff_dv_sat_fact_tech <> lag_hdiff or (hdiff_dv_sat_fact_tech <> sat_hdiff AND lag_hdiff IS NULL) OR sat_hdiff IS NULL
ORDER BY loadts
ON CONFLICT DO NOTHING;
