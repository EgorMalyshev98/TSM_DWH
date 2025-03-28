-------------------------------
-- STAGE LAYER
-------------------------------
DROP TABLE IF EXISTS stg_1c_works;
CREATE TABLE stg_1c_works(
	recsource varchar,
	loadts timestamptz,
	hkcode varchar,
	--
	hk_dv_hub_fact_journal bytea,
	hk_dv_hub_fact_work bytea,
	hk_dv_lnk_fact_journal_fact_work bytea,
	--
	bk_работа_uuid varchar,
	bk_жуфвр_uuid varchar,
	--
	hdiff_dv_sat_fact_work bytea,
	--
	идентификатор varchar,
	объем_работы numeric,
	примечание varchar,
	кв varchar,
	пометка_удаления bool,
	тип_spider varchar,
	структура_работ_value uuid,
	структура_работ_codе varchar,
	структура_работ_name varchar,
	видработ_value uuid,
	видработ_codе varchar,
	видработ_name varchar
);


DROP TABLE IF EXISTS stg_1c_journal;
CREATE TABLE stg_1c_journal(
	recsource varchar,
	loadts timestamptz,
	hkcode varchar,
	--
	hk_dv_hub_fact_journal bytea,
	--
	bk_жуфвр_uuid varchar,
	--
	hdiff_dv_sat_fact_journal bytea,
	--
	rabbit_uuid uuid,
	rabbit_дата timestamptz,
	проведен boolean,
	uuid_жуфвр uuid,
	пометка_удаления boolean,
	дата timestamptz,
	номер varchar,
	территория_value uuid,
	территория_codе varchar,
	территория_name varchar,
	подразделение_value uuid,
	подразделение_codе varchar,
	подразделение_name varchar,
	смена_value uuid,
	смена_codе varchar,
	смена_name varchar,
	прораб_value uuid,
	прораб_codе varchar,
	прораб_name varchar,
	комментарий varchar,
	ответственный_value uuid,
	ответственный_codе varchar,
	ответственный_name varchar,
	версия_жуфвр_value uuid,
	версия_жуфвр_codе varchar,
	версия_жуфвр_name varchar,
	направление_деятельности_value uuid,
	направление_деятельности_codе varchar,
	направление_деятельности_name varchar
);


DROP TABLE IF EXISTS stg_1c_pikets;
CREATE TABLE stg_1c_pikets(
	recsource varchar,
	loadts timestamptz,
	hkcode varchar,
	--
	hk_dv_hub_fact_work bytea,
	--
	bk_работа_uuid	 varchar,
	--
	hdiff_dv_msat_fact_pikets bytea,
	--
	ключстроки	 uuid,
	пикет_с	 numeric,
	пикет_по	 numeric,
	смещение_с	 numeric,
	смещение_по	 numeric,
	объем_работы	 numeric,
	вид_пикета_value	 uuid,
	вид_пикета_codе	 varchar,
	вид_пикета_name	 varchar,
	тип_пикета_value	 uuid,
	тип_пикета_codе	 varchar,
	тип_пикета_name	 varchar,
	группа_пикетов_spider_value	 uuid,
	группа_пикетов_spider_codе	 varchar,
	группа_пикетов_spider_name	 varchar
);


DROP TABLE IF EXISTS stg_1c_materials;
CREATE TABLE stg_1c_materials(
	recsource varchar,
	loadts timestamptz,
	hkcode varchar,
	--
	hk_dv_hub_fact_work bytea,
	--
	bk_работа_uuid	 varchar,
	--
	hdiff_dv_msat_fact_materials bytea,
	--
	номер_строки	 int,
	примечание	 varchar,
	объем_материала	 numeric,
	ресурс_value	 uuid,
	ресурс_codе	 varchar,
	ресурс_name	 varchar
);


DROP TABLE IF EXISTS stg_1c_tech;
CREATE TABLE stg_1c_tech(
	recsource varchar,
	loadts timestamptz,
	hkcode varchar,
	--
	hk_dv_hub_fact_work bytea,
	hk_dv_hub_tech bytea,
	hk_dv_lnk_fact_work_tech bytea,
	--
	bk_работа_uuid	 varchar,
	bk_гар_номер	 varchar,
	--
	hdiff_dv_sat_fact_tech bytea,
	--
	примечание	 varchar,
	госномер_техника_не_найдена	 varchar,
	количество	 int,
	часы	 numeric,
	ресурс_value	 uuid,
	ресурс_codе	 varchar,
	ресурс_name	 varchar,
	аналитика_value	 uuid,
	аналитика_codе	 varchar,
	аналитика_name	 varchar,
	контрагент_value	 varchar,
	контрагент_codе	 varchar,
	контрагент_name	 varchar
);

DROP TABLE IF EXISTS stg_1c_norm_workload;
CREATE TABLE stg_1c_norm_workload(
	recsource varchar,
	loadts timestamptz,
	hkcode varchar,
	--
	hk_dv_hub_fact_work bytea,
	--
	bk_работа_uuid	 varchar,
	--
	hdiff_dv_msat_norm_workload bytea,
	--
	период	 timestamptz,
	рассчетный_объем	 numeric,
	трудоемкость_нормативная	 numeric,
	трудоемкость_фактическая	 numeric,
	наемный_ресурс	 bool,
	ключевой_ресурс_value	 uuid,
    ключевой_ресурс_code	 varchar,
    ключевой_ресурс_name	 varchar,
	несколько_ключевых_ресурсов	 bool,
	жуфвр	 uuid,
	ключ_документа_ресурс	 uuid,
	структура_работ_value	 uuid,
	структура_работ_codе	 varchar,
	структура_работ_name	 varchar,
	ресурс_spider_value	 uuid,
	ресурс_spider_codе	 varchar,
	ресурс_spider_name	 varchar,
	аналитика_value	 uuid,
	аналитика_codе	 varchar,
	аналитика_name	 varchar
);

-------------------------------
-- DATA VAULT LAYER
-------------------------------
DROP TABLE IF EXISTS dv_hub_fact_work CASCADE;
CREATE TABLE IF NOT EXISTS dv_hub_fact_work(
    hk_dv_hub_fact_work BYTEA NOT NULL,
    hkcode VARCHAR NOT NULL,
    recsource VARCHAR NOT NULL,
    loadts TIMESTAMPTZ NOT NULL,
    bk_работа_uuid varchar NOT NULL,

    PRIMARY KEY(hk_dv_hub_fact_work)
);
CALL insert_ghost_record('dv_hub_fact_work');
--
DROP TABLE IF EXISTS dv_hub_fact_journal CASCADE;
CREATE TABLE IF NOT EXISTS dv_hub_fact_journal(
    hk_dv_hub_fact_journal BYTEA NOT NULL,
    hkcode VARCHAR NOT NULL,
    recsource VARCHAR NOT NULL,
    loadts TIMESTAMPTZ NOT NULL,
    bk_жуфвр_uuid varchar NOT NULL,

    PRIMARY KEY(hk_dv_hub_fact_journal)
);
CALL insert_ghost_record('dv_hub_fact_journal');
--
DROP TABLE IF EXISTS dv_hub_tech CASCADE;
CREATE TABLE IF NOT EXISTS dv_hub_tech(
    hk_dv_hub_tech BYTEA NOT NULL,
    hkcode VARCHAR NOT NULL,
    recsource VARCHAR NOT NULL,
    loadts TIMESTAMPTZ NOT NULL,
    bk_гар_номер varchar NOT NULL,
    PRIMARY KEY(hk_dv_hub_tech)
);
CALL insert_ghost_record('dv_hub_tech');
--
DROP TABLE IF EXISTS dv_lnk_fact_journal_fact_work CASCADE;
CREATE TABLE IF NOT EXISTS dv_lnk_fact_journal_fact_work(
    hk_dv_lnk_fact_journal_fact_work BYTEA NOT NULL,
    recsource VARCHAR NOT NULL,
    loadts TIMESTAMPTZ NOT NULL,
    hk_dv_hub_fact_journal BYTEA NOT NULL REFERENCES dv_hub_fact_journal(hk_dv_hub_fact_journal),
    hk_dv_hub_fact_work BYTEA NOT NULL REFERENCES dv_hub_fact_work(hk_dv_hub_fact_work),

    PRIMARY KEY(hk_dv_lnk_fact_journal_fact_work)
);
CALL insert_ghost_record('dv_lnk_fact_journal_fact_work');
--
DROP TABLE IF EXISTS dv_lnk_fact_work_tech CASCADE;
CREATE TABLE IF NOT EXISTS dv_lnk_fact_work_tech(
    hk_dv_lnk_fact_work_tech BYTEA NOT NULL,
    recsource VARCHAR NOT NULL,
    loadts TIMESTAMPTZ NOT NULL,
    hk_dv_hub_fact_work BYTEA NOT NULL REFERENCES dv_hub_fact_work(hk_dv_hub_fact_work),
    hk_dv_hub_tech BYTEA NOT NULL REFERENCES dv_hub_tech(hk_dv_hub_tech),

    PRIMARY KEY(hk_dv_lnk_fact_work_tech)
);
CALL insert_ghost_record('dv_lnk_fact_work_tech');
--
DROP TABLE IF EXISTS dv_sat_fact_work;
CREATE TABLE IF NOT EXISTS dv_sat_fact_work(
    hk_dv_hub_fact_work BYTEA NOT NULL REFERENCES dv_hub_fact_work(hk_dv_hub_fact_work),
    recsource VARCHAR NOT NULL,
    loadts TIMESTAMPTZ NOT NULL,
    hdiff_dv_sat_fact_work BYTEA NOT NULL,
    идентификатор varchar,
    объем_работы numeric,
    примечание varchar,
    кв varchar,
    пометка_удаления bool,
    тип_spider varchar,
    структура_работ_value uuid,
    структура_работ_codе varchar,
    структура_работ_name varchar,
    видработ_value uuid,
    видработ_codе varchar,
    видработ_name varchar,

    PRIMARY KEY(hk_dv_hub_fact_work, loadts)
);
CALL insert_ghost_record('dv_sat_fact_work');
--
DROP TABLE IF EXISTS dv_sat_fact_journal;
CREATE TABLE IF NOT EXISTS dv_sat_fact_journal(
    hk_dv_hub_fact_journal BYTEA NOT NULL REFERENCES dv_hub_fact_journal(hk_dv_hub_fact_journal),
    recsource VARCHAR NOT NULL,
    loadts TIMESTAMPTZ NOT NULL,
    hdiff_dv_sat_fact_journal BYTEA NOT NULL,
    rabbit_uuid uuid,
    rabbit_дата timestamptz,
    проведен boolean,
    uuid_жуфвр uuid,
    пометка_удаления boolean,
    дата timestamptz,
    номер varchar,
    территория_value uuid,
    территория_codе varchar,
    территория_name varchar,
    подразделение_value uuid,
    подразделение_codе varchar,
    подразделение_name varchar,
    смена_value uuid,
    смена_codе varchar,
    смена_name varchar,
    прораб_value uuid,
    прораб_codе varchar,
    прораб_name varchar,
    комментарий varchar,
    ответственный_value uuid,
    ответственный_codе varchar,
    ответственный_name varchar,
    версия_жуфвр_value uuid,
    версия_жуфвр_codе varchar,
    версия_жуфвр_name varchar,
    направление_деятельности_value uuid,
    направление_деятельности_codе varchar,
    направление_деятельности_name varchar,

    PRIMARY KEY(hk_dv_hub_fact_journal, loadts)
);
CALL insert_ghost_record('dv_sat_fact_journal');
--
DROP TABLE IF EXISTS dv_msat_fact_pikets;
CREATE TABLE IF NOT EXISTS dv_msat_fact_pikets(
    hk_dv_hub_fact_work BYTEA NOT NULL REFERENCES dv_hub_fact_work(hk_dv_hub_fact_work),
    recsource VARCHAR NOT NULL,
    loadts TIMESTAMPTZ NOT NULL,
    hdiff_dv_msat_fact_pikets BYTEA NOT NULL,
    sub_seq int NOT NULL,
    --
    bk_работа_uuid	 varchar,
    ключстроки	 uuid,
    пикет_с	 numeric,
    пикет_по	 numeric,
    смещение_с	 numeric,
    смещение_по	 numeric,
    объем_работы	 numeric,
    вид_пикета_value	 uuid,
    вид_пикета_codе	 varchar,
    вид_пикета_name	 varchar,
    тип_пикета_value	 uuid,
    тип_пикета_codе	 varchar,
    тип_пикета_name	 varchar,
    группа_пикетов_spider_value	 uuid,
    группа_пикетов_spider_codе	 varchar,
    группа_пикетов_spider_name	 varchar,

    PRIMARY KEY(hk_dv_hub_fact_work, loadts, sub_seq)
);
CALL insert_ghost_record('dv_msat_fact_pikets');
--
DROP TABLE IF EXISTS dv_msat_fact_materials;
CREATE TABLE IF NOT EXISTS dv_msat_fact_materials(
    hk_dv_hub_fact_work BYTEA NOT NULL REFERENCES dv_hub_fact_work(hk_dv_hub_fact_work),
    recsource VARCHAR NOT NULL,
    loadts TIMESTAMPTZ NOT NULL,
    hdiff_dv_msat_fact_materials BYTEA NOT NULL,
    sub_seq int NOT NULL,
    --
    bk_работа_uuid	 varchar,
    номер_строки	 int,
    примечание	 varchar,
    объем_материала	 numeric,
    ресурс_value	 uuid,
    ресурс_codе	 varchar,
    ресурс_name	 varchar,

    PRIMARY KEY(hk_dv_hub_fact_work, loadts, sub_seq)

);
CALL insert_ghost_record('dv_msat_fact_materials');
--
DROP TABLE IF EXISTS dv_sat_fact_tech;
CREATE TABLE IF NOT EXISTS dv_sat_fact_tech(
    hk_dv_lnk_fact_work_tech BYTEA NOT NULL REFERENCES dv_lnk_fact_work_tech(hk_dv_lnk_fact_work_tech),
    recsource VARCHAR NOT NULL,
    loadts TIMESTAMPTZ NOT NULL,
    hdiff_dv_sat_fact_tech BYTEA NOT NULL,
    --
    примечание	 varchar,
    госномер_техника_не_найдена	 varchar,
    количество	 int,
    часы	 numeric,
    ресурс_value	 uuid,
    ресурс_codе	 varchar,
    ресурс_name	 varchar,
    аналитика_value	 uuid,
    аналитика_codе	 varchar,
    аналитика_name	 varchar,
    контрагент_value    varchar,
    контрагент_codе	 varchar,
    контрагент_name	 varchar,

    PRIMARY KEY(hk_dv_lnk_fact_work_tech, loadts)
);
CALL insert_ghost_record('dv_sat_fact_tech');
--
DROP TABLE IF EXISTS dv_msat_norm_workload;
CREATE TABLE IF NOT EXISTS dv_msat_norm_workload(
    hk_dv_hub_fact_work BYTEA NOT NULL REFERENCES dv_hub_fact_work(hk_dv_hub_fact_work),
    recsource VARCHAR NOT NULL,
    loadts TIMESTAMPTZ NOT NULL,
    hdiff_dv_msat_norm_workload BYTEA NOT NULL,
    sub_seq int NOT NULL,
    --
    bk_работа_uuid	 varchar,
    период	 timestamptz,
    рассчетный_объем	 numeric,
    трудоемкость_нормативная	 numeric,
    трудоемкость_фактическая	 numeric,
    наемный_ресурс	 bool,
    ключевой_ресурс_value	 uuid,
    ключевой_ресурс_code	 varchar,
    ключевой_ресурс_name	 varchar,
    несколько_ключевых_ресурсов	 bool,
    жуфвр	 uuid,
    ключ_документа_ресурс	 uuid,
    структура_работ_value	 uuid,
    структура_работ_codе	 varchar,
    структура_работ_name	 varchar,
    ресурс_spider_value	 uuid,
    ресурс_spider_codе	 varchar,
    ресурс_spider_name	 varchar,
    аналитика_value	 uuid,
    аналитика_codе	 varchar,
    аналитика_name	 varchar,

    PRIMARY KEY(hk_dv_hub_fact_work, loadts, sub_seq)

);
CALL insert_ghost_record('dv_msat_norm_workload');
--