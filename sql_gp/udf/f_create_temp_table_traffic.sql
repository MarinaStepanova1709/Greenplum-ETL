CREATE OR REPLACE FUNCTION std12_116_pr.f_create_temp_table_traffic(
    p_table         text,       -- 'std12_116_pr.traffic'
    p_pxf_table     text,       -- 'gp.traffic'
    p_user_id       text,       -- JDBC user
    p_pass          text,       -- JDBC pass
    p_partition_key text, 
    p_start_date    timestamp,  -- нижняя граница по date_tr
    p_end_date      timestamp   -- верхняя граница (не включительно)
)
RETURNS int8
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_ext_table  text := p_table || '_ext';
    v_temp_table text := p_table || '_temp';
    v_sql        text;
    v_pxf        text;
    v_dist_key   text;
    v_params     text;
    v_table_oid  oid;
    v_cnt        int8;
begin
	v_ext_table  := p_table || '_ext';
    v_temp_table := p_table || '_temp';

	-- 1) Удаляем старые 
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
  	v_sql := ' CREATE EXTERNAL TABLE ' || v_ext_table || ' (
        plant     varchar(10)  , 
		 "date"    varchar(10)  ,
        "time"    bpchar(6)  ,
        frame_id  bpchar(10),
        quantity  int4   
      )
      LOCATION (''' || v_pxf || ''')
      ON ALL
      FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
      ENCODING ''UTF8''' ;
   
    RAISE NOTICE '[CREATE EXT] %', v_sql;
    EXECUTE v_sql;
    
        -- Создаем временную таблицу с той же структурой и параметрами
-- Создаем временную таблицу с той же структурой и параметрами
      execute  'drop table if exists ' || v_temp_table;
    
    v_sql = 'create table ' || v_temp_table || 
    ' (like ' ||p_table || ') ' || v_params || ' ' || v_dist_key|| ';' ;
    
    RAISE NOTICE 'CREATE TEMP TABLE SQL: %', v_sql;
    execute v_sql;
    
     -- Копируем данные конвертируя date+time в  timestamp date_tr
    v_sql = 'insert into ' || v_temp_table || 
    	' select plant,  to_date("date", ''DD.MM.YYYY'')  AS "date",  to_timestamp("time", ''HH24MISS'')::time     AS "time",
			to_timestamp("date" || "time", ''DD.MM.YYYYHH24MISS'') AS date_tr,
				frame_id, quantity
		from ' || v_ext_table ||
		'	where to_timestamp("date"||"time", ''DD.MM.YYYYHH24MISS'') 
				>= '''  || p_start_date ||'''::TIMESTAMP 
				and 
				to_timestamp("date"||"time", ''DD.MM.YYYYHH24MISS'') < ''' || p_end_date || '''::timestamp'   ;
    raise notice 'sql_insert:  %' , v_sql;
     
    EXECUTE v_sql;

	 --GET DIAGNOSTICS показывает сколько строк было удалено\добавлено в последнем запросе
    GET DIAGNOSTICS v_cnt = ROW_COUNT;
   	RAISE NOTICE 'Вставлено строк в временную таблицу: %', v_cnt;

 	RETURN v_cnt;
END;
$$
EXECUTE ON ANY;



SELECT std12_116_pr.f_create_temp_table_traffic(
  'std12_116_pr.traffic',               -- p_table
  'gp.traffic',                      -- p_pxf_table
  'intern',                          -- p_user_id
  'intern',                          -- p_pass
  'date_tr',
  '2021-01-01 00:00:00'::timestamp,  -- p_start_date
  '2021-03-01 00:00:00'::timestamp   -- p_end_date
);

select * from std12_116_pr.traffic_temp;

