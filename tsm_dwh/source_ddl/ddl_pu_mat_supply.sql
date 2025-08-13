DROP TABLE IF EXISTS src_pu_mat_supply CASCADE;
CREATE TABLE src_pu_mat_supply(
	id serial primary key,

	recsource varchar,
	loadts timestamptz,

    "Наименование объекта" varchar,
    "Наименование материала" varchar,
    "date" date,
    "Единица измерения" varchar,
    "volume" numeric,
    "План суточный" numeric
)



