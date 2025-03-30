INSERT INTO dv_hub_fact_work (hk_dv_hub_fact_work, hkcode, recsource, loadts, bk_работа_uuid)
SELECT DISTINCT
    hk_dv_hub_fact_work,
    hkcode,
    recsource,
    loadts,
    bk_работа_uuid
FROM stg_1c_works
ON CONFLICT DO NOTHING;
