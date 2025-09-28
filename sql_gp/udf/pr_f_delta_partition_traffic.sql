CREATE OR REPLACE FUNCTION std12_116_pr.f_delta_partition_traffic(p_table_from text, p_pxf_table  text,  
								p_user_id   text,   p_pass  text,  p_table_to text, p_partition_key text,
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

			-- 2) загружаем дельту в  (_temp) и получаем количество строк
        SELECT std12_116_pr.f_create_temp_table_traffic(
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
		
				-- обмениваем партицию
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


SELECT std12_116_pr.f_delta_partition_traffic(
  'std12_116_pr.traffic',                  -- p_table_from
  'gp.traffic',                         -- pxf-таблица
  'intern',                             -- p_user_id
  'intern',                             -- p_pass
  'std12_116_pr.traffic',                  -- p_table_to
  '"date"',
  '2021-01-01 00:00:00'::timestamp,     -- начало диапазона
  '2021-03-01 00:00:00'::timestamp      -- конец диапазона (не включ.)
);


CREATE OR REPLACE FUNCTION std12_116_pr.f_delta_partition_traffic(p_table_from text, p_pxf_table  text,    p_user_id   text,   p_pass  text,  p_table_to text, p_partition_key text,
							p_start_date timestamp, p_end_date timestamp)
							
							
select * from std12_116_pr.traffic;

select gp_segment_id, count(*)
from std12_116_pr.traffic
group by 1

-- 2) Коэффициент разбалансировки
SELECT
  (gp_toolkit.gp_skew_coefficient('std12_116_pr.traffic'::regclass)).skccoeff;

ALTER TABLE std12_116_pr.traffic  
SET WITH (reorganize = true)
DISTRIBUTED RANDOMLY;



