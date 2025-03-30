INSERT INTO dv_hub_tech (hk_dv_hub_tech, hkcode, recsource, loadts, bk_гар_номер)
SELECT DISTINCT
    hk_dv_hub_tech,
    hkcode,
    recsource,
    loadts,
    bk_гар_номер
FROM stg_1c_tech
ON CONFLICT DO NOTHING;
