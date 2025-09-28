
CREATE OR REPLACE FUNCTION std12_116_pr.f_create_temp_table_coupons( p_table text, p_file_name text,
					  p_partition_key text, p_start_date timestamp, p_end_date timestamp)
	RETURNS int8 
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare 
	v_ext_table text;            -- Имя внешней таблицы
    v_temp_table text;           -- Имя временной таблицы
    v_sql text;
    v_gpfdist text;
    v_dist_key text;
    v_params text;
    v_table_oid int4;
   -- v_columns text;
  --  v_delete_query text;
 --   v_insert_query text;
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
        v_dist_key := 'DISTRIBUTED RANDOMLY';
    ELSE
        -- Иначе получаем её ключ распределения
    v_dist_key := pg_get_table_distributedby(v_table_oid);
    end if;
       
     -- Получаем параметры хранения таблицы (например, сжатие, ориентация)
    SELECT COALESCE('with (' || ARRAY_TO_STRING(reloptions, ', ') || ')', '')
    INTO v_params
    FROM pg_class
    WHERE oid = v_table_oid;

    
    	-- Формируем строку подключения через gpf_dist
	v_gpfdist := 'gpfdist://172.16.128.150:8080/' || p_file_name || '.CSV';
    	
    raise notice 'v_gpfdist_connection: %', v_gpfdist;

    
     -- Создаём команду для создания внешней таблицы с той же структурой, что и основная
    v_sql := 'CREATE EXTERNAL TABLE ' || v_ext_table || ' (
		plant bpchar(4),
	    "date" varchar (10),
	    coupon_num bpchar(8),
	    promo_id varchar (80),
	    material int8,
	    billnum int8)
    LOCATION (''' || v_gpfdist || ''')
    ON ALL
    FORMAT ''CSV'' (HEADER DELIMITER '';'' NULL '''' ESCAPE ''"'' QUOTE ''"'' )
    ENCODING ''UTF8'' ';
    
    raise notice 'create v_sql: %' , v_sql;
    
    execute v_sql;
    
    
-- Создаем временную таблицу с той же структурой и параметрами
      execute  'drop table if exists ' || v_temp_table;
    
    v_sql = 'create table ' || v_temp_table || 
    ' (like ' ||p_table || ') ' || v_params || ' ' || v_dist_key|| ';' ;
    
    RAISE NOTICE 'CREATE TEMP TABLE SQL: %', v_sql;
    execute v_sql;
    
     -- Копируем данные из внешней таблицы во временную
    v_sql = 'insert into ' || v_temp_table || 
    	' select plant,
				  to_date("date", ''YYYYMMDD'') AS date,
				  trim(coupon_num) AS coupon_num,
				  promo_id,
				  material,
				  billnum
		 from ' || v_ext_table ||
		' WHERE to_date(' || p_partition_key || ', ''YYYYMMDD'') >= ''' 
        || p_start_date || '''::timestamp' ||
    '   AND to_date(' || p_partition_key || ', ''YYYYMMDD'') <  ''' 
        || p_end_date   || '''::timestamp';
    
raise notice 'sql_insert:  %' , v_sql;
     
    EXECUTE v_sql;

	 --GET DIAGNOSTICS показывает сколько строк было удалено\добавлено в последнем запросе
    GET DIAGNOSTICS v_cnt = ROW_COUNT;
   	RAISE NOTICE 'Вставлено строк в временную таблицу: %', v_cnt;

 	RETURN v_cnt;
END;
$$
EXECUTE ON ANY;


					  
select std12_116_pr.f_create_temp_table_coupons ('std12_116_pr.coupons', 'coupons', '"date"',  '2021-01-01'::timestamp, '2021-02-01' ::timestamp)					  
					 
select  length(coupon_num) AS char_count, *, length(trim(coupon_num)) from std12_116_pr.coupons_temp		    

			 