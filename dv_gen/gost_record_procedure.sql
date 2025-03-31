CREATE OR REPLACE PROCEDURE insert_ghost_record(target_table TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    col_list TEXT;
    val_list TEXT;
    sql_stmt TEXT;
BEGIN

    SELECT 
        string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position) AS cols,
        string_agg(
            CASE 
				WHEN column_name LIKE 'hdiff_%' THEN 'decode(repeat(''00'', 20), ''hex'')'
                WHEN column_name LIKE 'hk_%' THEN 'decode(repeat(''00'', 20), ''hex'')'
                WHEN column_name LIKE 'bk_%' THEN '''GHOST'''
                WHEN column_name = 'loadts' THEN '''1900-01-01 00:00:00+00'''
                WHEN column_name = 'hkcode' THEN '''default'''
				WHEN column_name = 'recsource' THEN '''GHOST'''
                WHEN column_name = 'sub_seq' THEN '''1'''
				
            END, ', ' ORDER BY ordinal_position
        ) AS vals
    INTO col_list, val_list
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = target_table
      AND (column_name LIKE 'hk_%' OR column_name LIKE 'bk_%' OR column_name LIKE 'hdiff_%' OR column_name = 'loadts' OR column_name = 'recsource' OR column_name = 'hkcode'  OR column_name = 'sub_seq');

    IF col_list IS NULL OR val_list IS NULL THEN
        RAISE NOTICE 'Нет колонок для ghost record в таблице %', target_table;
        RETURN;
    END IF;

    sql_stmt := format(
        'INSERT INTO public.%I (%s) VALUES (%s);',
        target_table, col_list, val_list
    );
    EXECUTE sql_stmt;
END;
$$;
