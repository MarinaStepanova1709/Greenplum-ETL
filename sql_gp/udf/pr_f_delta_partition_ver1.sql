CREATE OR REPLACE FUNCTION std12_116_pr.f_delta_partition(p_table_from text, p_pxf_table  text,    p_user_id   text,   p_pass  text,  p_table_to text, p_partition_key text,
							p_start_date timestamp, p_end_date timestamp)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
	v_table_from text;
	v_table_to text;
	v_sql text;
	v_load_interval interval;
	v_iter_date timestamp;
	v_where text;
    v_dist_key text;
    v_params text;
    v_columns text;
    v_delete_query text;
    v_insert_query text;
	v_month_cnt    int8;
    v_cnt           int8 := 0; 
	v_start_date timestamp;
	v_end_date timestamp;
	v_temp_table   text := p_table_from || '_temp';
	v_partition    text;
begin
	
	perform std12_116_pr.f_create_date_partition (p_table_to,  p_end_date:: timestamp);
	v_load_interval = '1 month' :: interval;
	
	v_start_date := date_trunc('month', p_start_date );
	v_end_date := date_trunc('month', p_end_date );

	
		loop
			v_iter_date := v_start_date + v_load_interval;
			exit when  (v_iter_date > v_end_date );

			-- 2) загружаем дельту в staging (_temp) и получаем количество строк
        SELECT std12_116_pr.f_create_temp_table(
            p_table_from,
            p_pxf_table,
            p_user_id,
            p_pass,
            p_partition_key,
            v_start_date::timestamp,
            v_iter_date::timestamp
        )
        INTO v_month_cnt;
        
		v_cnt := v_cnt + v_month_cnt;
		
				-- обьениваем партицию
			v_sql :=  'ALTER TABLE ' || p_table_to ||
			' EXCHANGE PARTITION FOR (DATE ''' ||
 				to_char(v_start_date, 'YYYY-MM-DD') ||
  			''') WITH TABLE ' || v_temp_table || ' WITH VALIDATION';
			raise notice 'sql_insert:  %' , v_sql;
     
    		EXECUTE v_sql;
			-- 5) переключаемся на следующий месяц
			v_start_date := v_iter_date;
		end loop;
	return v_cnt;
		
end;
$$
execute on any;

sql_insert:  alter table std12_116_pr.bills_item exchange partition for (date '2021-02-01 00:00:00::TIMESTAMP ') with table std12_116_pr.bills_item_temp with validation


SELECT std12_116_pr.f_delta_partition(
  -- источник / staging
  'std12_116_pr.bills_item',     -- p_table_from 
  'gp.bills_item',            -- p_pxf_table 
  'intern',                   -- p_user_id 
  'intern',                   -- p_pass 
  -- цель / родительская
  'std12_116_pr.bills_item',     -- p_table_to 
  'calday',                   -- p_partition_key
  -- диапазон подгрузки
  '2021-01-01 00:00:00'::timestamp,  -- p_start_date
  '2021-03-01 00:00:00'::timestamp   -- p_end_date (не включительно)
);


select * 
from std12_116_pr.bills_item

select  std12_116_pr.f_delta_partition( 'std12_116_pr.bills_head' , 'gp.bills_head' , 'intern',
'intern', 'std12_116_pr.bills_head', 'calday', '2021-02-01'::timestamp, '2021-03-02'::timestamp)


