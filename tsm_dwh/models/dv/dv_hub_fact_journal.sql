INSERT INTO dv_hub_fact_journal (hk_dv_hub_fact_journal, hkcode, recsource, loadts, bk_жуфвр_uuid)
SELECT DISTINCT
    hk_dv_hub_fact_journal,
    hkcode,
    recsource,
    loadts,
    bk_жуфвр_uuid
FROM stg_1c_journal
ON CONFLICT DO NOTHING;
