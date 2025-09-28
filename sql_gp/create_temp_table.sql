
CREATE OR REPLACE FUNCTION std12_116_pr.f_create_temp_table( p_table text, p_pxf_table text,
					 p_user_id text, p_pass text, p_partition_key text, p_start_date timestamp, p_end_date timestamp)
	RETURNS int8 
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare 
	v_ext_table text;            -- Имя внешней таблицы
    v_temp_table text;           -- Имя временной таблицы
    v_sql text;
    v_pxf text;
    v_dist_key text;
    v_params text;
    v_table_oid int4;
    v_columns text;
    v_delete_query text;
    v_insert_query text;
    v_cnt int8;
begin
	v_ext_table  := p_table || '_ext';
    v_temp_table := p_table || '_temp';

	-- 1) Удаляем старые слои, если они есть
    EXECUTE 'DROP TABLE IF EXISTS ' || v_temp_table;
    EXECUTE 'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_table;

  raise notice 'DROP EXTERNAL TABLE IF EXISTS %' , v_ext_table;

 --Далее достаем ключ распределения целовой таблицы, для этого 
    -- Получаем OID основной таблицы и потом передадим в системную функц
	select c.oid
	into v_table_oid
	from pg_class as c 
	inner join pg_namespace as n on c.relnamespace = n.oid 
	where n.nspname || '.' || c.relname = p_table
	limit 1;
     
       -- Если таблица не найдена, задаём случайное распределение
    IF v_table_oid = 0 OR v_table_oid IS NULL THEN
        v_dist_key = 'DISTRIBUTED RANDOMLY';
    ELSE
        -- Иначе получаем её ключ распределения
    v_dist_key = pg_get_table_distributedby(v_table_oid);
    end if;
       
     -- Получаем параметры хранения таблицы (например, сжатие, ориентация)
    SELECT COALESCE('with (' || ARRAY_TO_STRING(reloptions, ', ') || ')', '')
    INTO v_params
    FROM pg_class
    WHERE oid = v_table_oid;

    
    	-- Формируем строку подключения через PXF
    v_pxf = 'pxf://' || p_pxf_table || '?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver' ||
    	'&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=' ||
    	p_user_id || '&PASS=' || p_pass;
    	
    raise notice 'PXF connection: %', v_pxf;
    
        -- Создаем внешнюю таблицу
    v_sql = 'create external table ' || v_ext_table || 
    	' (like ' || p_table || ')
		LOCATION (''' || v_pxf || ''')
		ON ALL
		FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
		ENCODING ''UTF8'' ';
    
    raise notice 'create v_sql: %' , v_sql;
    
    execute v_sql;
    
        -- Создаем временную таблицу с той же структурой и параметрами
-- Создаем временную таблицу с той же структурой и параметрами
      execute  'drop table if exists ' || v_temp_table;
    
    v_sql = 'create table ' || v_temp_table || 
    ' (like ' ||p_table || ') ' || v_params || ' ' || v_dist_key|| ';' ;
    
    RAISE NOTICE 'CREATE TEMP TABLE SQL: %', v_sql;
    execute v_sql;
    
     -- Копируем данные из внешней таблицы во временную
    v_sql = 'insert into ' || v_temp_table || 
    	' select * from ' || v_ext_table ||
		'	where ' || p_partition_key || ' >= '''  || p_start_date ||'''::TIMESTAMP and ' || p_partition_key || ' < ''' || p_end_date || '''::timestamp'   ;
    raise notice 'sql_insert:  %' , v_sql;
     
    EXECUTE v_sql;

	 --GET DIAGNOSTICS показывает сколько строк было удалено\добавлено в последнем запросе
    GET DIAGNOSTICS v_cnt = ROW_COUNT;
   	RAISE NOTICE 'Вставлено строк в временную таблицу: %', v_cnt;

 	RETURN v_cnt;
END;
$$
EXECUTE ON ANY;


select * 
from std12_116_pr.bills_item_temp

select  std12_116_pr.f_create_temp_table (  'std12_116_pr.bills_item',  'gp.bills_item',   'intern',  'intern', 'calday', '2021-01-01', '2021-03-01')


					 
					 

			 