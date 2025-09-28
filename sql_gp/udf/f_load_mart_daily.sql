-- функция построения динамической витрины plan_fact_YYYYMM без format(), через конкатенацию строк
create or replace function std12_116_pr.f_load_mart_daily(p_from varchar, p_to varchar)
returns int4
language plpgsql
volatile
as $$
declare
    v_table     text := 'sales_report_daily_' || p_from || '_'|| p_to;        -- суффикс имени таблицы
    v_full_name text := 'std12_116_pr.' || v_table;        -- полное имя таблицы
    v_sql       text;                                   -- динамический SQL
    v_return    int;                                    -- итоговый count(*)
begin
    -- 1) удалить старую витрину, если есть
    execute 'drop table if exists ' || v_full_name;

    -- 2) собрать и выполнить create table ... as ... через конкатенацию
  v_sql := 
	'create table ' || v_full_name || ' AS
	with cte1 as (
			SELECT trim(plant) as plant,date(bh.calday) as dt , sum (rpa_sat) as revenue , sum (qty) as sum_goods,  count(distinct bh.billnum) as qty_bills
			FROM std12_116_pr.bills_item AS bi
			join std12_116_pr.bills_head bh on bh.billnum = bi.billnum
			where date(bh.calday) >=  to_date(''' || p_from || ''',''YYYYMMDD'') and date(bh.calday) < to_date(''' || p_to || ''',''YYYYMMDD'')
			group by bh.plant , date(bh.calday)
		),
		first_bi AS (
		  -- находим для каждой пары (billnum, material) минимальный billitem
		  SELECT
		    billnum,
		    material,
		    MIN(billitem) AS billitem
		  FROM std12_116_pr.bills_item bil
		  where date(bil.calday) >=  to_date(''' || p_from || ''',''YYYYMMDD'') and date(bil.calday) < to_date(''' || p_to || ''',''YYYYMMDD'')
		  GROUP BY billnum, material
		),
		bi AS (
		  -- по этому минимальному billitem подтягиваем цену
		  SELECT
		    f.billnum,
		    bi.calday as dt,
		    f.material,
		    bi.rpa_sat::numeric / bi.qty AS price
		  FROM first_bi AS f
		  JOIN std12_116_pr.bills_item AS bi
		    ON bi.billnum   = f.billnum
		   AND bi.material  = f.material
		   AND bi.billitem  = f.billitem
		),
		-- Сумма скидок и число купонов по plant × dt
		cte2 as (
			select   c.plant, bi.dt,
			  SUM(
			    CASE
			      WHEN p.promo_type = ''002'' THEN bi.price * p.discount / 100
			      WHEN p.promo_type = ''001'' THEN p.discount
			    END
			  ) AS sum_disc,
			  COUNT(1) AS cnt_coupons
			FROM std12_116_pr.coupons   AS c
			LEFT JOIN bi
			  ON bi.billnum  = c.billnum
			 AND bi.material = c.material
			LEFT JOIN std12_116_pr.promotions AS p
			  ON p.promo_id = c.promo_id
			where date(bi.dt) >=  to_date(''' || p_from || ''',''YYYYMMDD'') and date(bi.dt) < to_date(''' || p_to || ''',''YYYYMMDD'')
			GROUP BY c.plant, bi.dt
		),
		-- 5. Трафик по plant × dt
		traf as
			(select plant, tr."date" as dt , sum(tr.quantity ) sum_traf from std12_116_pr.traffic tr
			where tr."date" >=  to_date(''' || p_from || ''',''YYYYMMDD'') and date(tr."date") < to_date(''' || p_to || ''',''YYYYMMDD'')
			group by plant , tr."date"
			)
		select  c1.plant, c1.dt, c1.revenue,
			COALESCE(c2.sum_disc, 0) as sum_disc, 
			 c1.revenue - COALESCE(c2.sum_disc,0) AS sales_wo_discount,
			sum_goods ,
			qty_bills ,
			COALESCE(t.sum_traf, 0)             AS sum_traf,
			COALESCE(c2.cnt_coupons,0)          AS cnt_coupons,
			COALESCE(  c2.cnt_coupons / NULLIF(c1.sum_goods,0) * 100  ,0) AS "доляТоваровПоАкции",
			COALESCE(   c1.sum_goods::float / NULLIF(c1.qty_bills,0)  ,0) AS "среднКолТовВЧеке",
			COALESCE(  c1.qty_bills / NULLIF(t.sum_traf,0) * 100.0 ,0) AS "КонверсМагазина",
			COALESCE(  c1.revenue / NULLIF(c1.qty_bills,0) ,0) AS "СреднЧек",
			COALESCE(  c1.revenue / NULLIF(t.sum_traf,0)  ,0) AS "СрВыручкНаПокупателя"
		from cte1 c1
		left join cte2 c2 USING (plant, dt)
		left join traf t USING (plant, dt)'
		 ;
    
raise notice 'sql_:  %' , v_sql;
execute v_sql;

    -- 3) посчитать число строк в новой витрине
    execute 'select count(*) from ' || v_full_name into v_return;

    -- 4) вернуть число строк
    return v_return;
end;
$$
execute on any;
		
explain analyze
select std12_116_pr.f_load_mart_daily('20210101', '20210301')

select * from 
std12_116_pr.sales_report_daily_20210101_20210301
order by plant, dt
