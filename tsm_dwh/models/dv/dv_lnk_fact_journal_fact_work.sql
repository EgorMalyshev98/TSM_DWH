INSERT INTO dv_lnk_fact_journal_fact_work
SELECT DISTINCT
    hk_dv_lnk_fact_journal_fact_work,
    recsource,
    loadts,
    hk_dv_hub_fact_journal,
	hk_dv_hub_fact_work
FROM stg_1c_works
ON CONFLICT DO NOTHING;
