DROP TABLE IF EXISTS ref_pu_materials_name;
CREATE TABLE ref_pu_materials_name(
	id serial primary key,
	имя_пу varchar,
	ед_изм varchar
)

-- внешние таблицы backend
CREATE OR REPLACE VIEW ref_objects AS (
	SELECT
		o.id AS object_id,
		o.code,
		o.name,
		o.is_active,
		d.name_1c
	FROM backend.objects o
	JOIN backend.object_dwh_mapping d ON o.id = d.object_id
	)



