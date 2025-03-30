INSERT INTO dv_lnk_fact_work_tech
SELECT DISTINCT
    hk_dv_lnk_fact_work_tech,
    recsource,
    loadts,
    hk_dv_hub_fact_work,
	hk_dv_hub_tech
FROM stg_1c_tech
ON CONFLICT DO NOTHING;
