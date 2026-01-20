DROP TABLE IF EXISTS ref_object_names;
CREATE TABLE ref_object_names(
	территория_value varchar,
	территория_name varchar,
	spider_name varchar,
	is_active bool
);


DROP TABLE IF EXISTS ref_glossary_object;
CREATE TABLE ref_glossary_object(
	id serial primary key,
	код varchar,
	bk varchar,
	имя_эу varchar,
	имя_пу varchar,
	имя_оуп varchar,
	имя_1с varchar,
	подразделение varchar,
	статус varchar,
	коммент varchar
);



DROP TABLE IF EXISTS ref_pu_materials_name;
CREATE TABLE ref_pu_materials_name(
	id serial primary key,
	имя_пу varchar,
	ед_изм varchar
)




