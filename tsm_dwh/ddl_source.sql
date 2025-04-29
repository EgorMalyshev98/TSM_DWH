-----------------------
-- ЖУФВР
-----------------------
DROP TABLE IF EXISTS src_journal CASCADE;
CREATE TABLE src_journal(
	recsource varchar,
	loadts timestamptz,

	uuid uuid,
	date timestamptz,
	"value_Проведен" boolean,
	"value_Ссылка" uuid,
	"value_ПометкаУдаления" boolean,
	"value_Дата" timestamptz,
	"value_Номер" varchar,
	"value_Территория_value" uuid,
	"value_Территория_codе" varchar,
	"value_Территория_name" varchar,
	"value_Подразделение_value" uuid,
	"value_Подразделение_codе" varchar,
	"value_Подразделение_name" varchar,
	"value_Смена_value" uuid,
	"value_Смена_codе" varchar,
	"value_Смена_name" varchar,
	"value_Прораб_value" uuid,
	"value_Прораб_codе" varchar,
	"value_Прораб_name" varchar,
	"value_Комментарий" varchar,
	"value_Ответственный_value" uuid,
	"value_Ответственный_codе" varchar,
	"value_Ответственный_name" varchar,
	"value_АктуальнаяВерсияЖУФВР_value" uuid,
	"value_АктуальнаяВерсияЖУФВР_codе" varchar,
	"value_АктуальнаяВерсияЖУФВР_name" varchar,
	"value_НаправлениеДеятельности_value" uuid,
	"value_НаправлениеДеятельности_codе" varchar,
	"value_НаправлениеДеятельности_name" varchar
);

DROP TABLE IF EXISTS src_works CASCADE;
CREATE TABLE src_works(
	recsource varchar,
	loadts timestamptz,
	--
	"value_Ссылка" uuid,
	"КлючСвязи" uuid,
	"СтруктураРаботИдентификатор" varchar,
	"ОбъемРаботы" numeric,
	"Примечание" varchar,
	"СтруктураРаботНомерПоКонтрактнойВедомости" varchar,
	"СтруктураРаботПометкаУдаления" bool,
	"ВидРаботУровеньОперации" varchar,
	"СтруктураРабот_value" uuid,
	"СтруктураРабот_codе" varchar,
	"СтруктураРабот_name" varchar,
	"ВидРабот_value" uuid,
	"ВидРабот_codе" varchar,
	"ВидРабот_name" varchar
);

DROP TABLE IF EXISTS src_pikets CASCADE;
CREATE TABLE src_pikets(
	recsource varchar,
	loadts timestamptz,
	--
	"КлючСвязи" uuid,
	"КлючСтроки" uuid,
	"ПикетС" numeric,
	"ПикетПо" numeric,
	"СмещениеС" numeric,
	"СмещениеПо" numeric,
	"Объем" numeric,
	"ВидПикета_value" uuid,
	"ВидПикета_codе" varchar,
	"ВидПикета_name" varchar,
	"ТипПикета_value" uuid,
	"ТипПикета_codе" varchar,
	"ТипПикета_name" varchar,
	"ГруппаПикетовSpider_value" uuid,
	"ГруппаПикетовSpider_codе" varchar,
	"ГруппаПикетовSpider_name" varchar
);

DROP TABLE IF EXISTS src_tech CASCADE;
CREATE TABLE src_tech(
	recsource varchar,
	loadts timestamptz,
	--
	"КлючСвязи" uuid,
	"Примечание" varchar,
	"ГосНомерТехникаНеНайдена" varchar,
	"Количество" int,
	"Часы" numeric,
	"Ресурс_value" uuid,
	"Ресурс_codе" varchar,
	"Ресурс_name" varchar,
	"Аналитика_value" uuid,
	"Аналитика_codе" varchar,
	"Аналитика_name" varchar,
	"Контрагент_value" varchar,
	"Контрагент_codе" varchar,
	"Контрагент_name" varchar
);

DROP TABLE IF EXISTS src_materials CASCADE;
CREATE TABLE src_materials(
	recsource varchar,
	loadts timestamptz,
	--
	"КлючСвязи" uuid,
	"НомерСтроки" int,
	"Примечание" varchar,
	"ОбъемМатериала" numeric,
	"Ресурс_value" uuid,
	"Ресурс_codе" varchar,
	"Ресурс_name" varchar
);

DROP TABLE IF EXISTS src_norm_workload CASCADE;
CREATE TABLE src_norm_workload(
	recsource varchar,
	loadts timestamptz,
	--
	"Регистратор" uuid,
	"Период" timestamptz,
	"РассчетныйОбъем" numeric,
	"ТрудоемкостьНормативная" numeric,
	"ТрудоемкостьФактическая" numeric,
	"НаемныйРесурс" bool,
	"КлючевойРесурс_value" uuid,
	"КлючевойРесурс_codе" varchar,
	"КлючевойРесурс_name" varchar,
	"НесколькоКлючевыхРесурсов" bool,
	"ЖУФВР" uuid,
	"КлючДокументаРесурс" uuid,
	"СтруктураРабот_value" uuid,
	"СтруктураРабот_codе" varchar,
	"СтруктураРабот_name" varchar,
	"РесурсSpider_value" uuid,
	"РесурсSpider_codе" varchar,
	"РесурсSpider_name" varchar,
	"Аналитика_value" uuid,
	"Аналитика_codе" varchar,
	"Аналитика_name" varchar
);


	
	
	