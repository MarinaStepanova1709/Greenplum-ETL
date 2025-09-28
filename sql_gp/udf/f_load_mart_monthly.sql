CREATE OR REPLACE FUNCTION std12_116_pr.f_load_mart_monthly(p_from VARCHAR, p_to VARCHAR)
RETURNS INT4
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_table     TEXT := 'sales_report_monthly_' || p_from || '_' || p_to;
    v_full_name TEXT := 'std12_116_pr.' || v_table;
    v_sql       TEXT;
    v_return    INT;
BEGIN
    -- 1) удалить старую витрину
    EXECUTE 'DROP TABLE IF EXISTS ' || v_full_name;

    -- 2) собрать CREATE TABLE AS … с группировками по началу месяца
    v_sql :=
      'CREATE TABLE ' || v_full_name || ' AS
       WITH cte1 AS (
         SELECT
           bh.plant,
           DATE_TRUNC(''month'', bh.calday)::date AS month_dt,
           SUM(bi.rpa_sat)       AS revenue,
           SUM(bi.qty)           AS sum_goods,
           COUNT(DISTINCT bh.billnum) AS qty_bills
         FROM std12_116_pr.bills_item bi
         JOIN std12_116_pr.bills_head bh
           ON bi.billnum = bh.billnum
         WHERE bh.calday >= TO_DATE(''' || p_from || ''',''YYYYMMDD'')
           AND bh.calday <  TO_DATE(''' || p_to   || ''',''YYYYMMDD'')
         GROUP BY bh.plant, DATE_TRUNC(''month'', bh.calday)
       ),
       first_bi AS (
         SELECT
           billnum,
           material,
           MIN(billitem) AS billitem
         FROM std12_116_pr.bills_item
         WHERE calday >= TO_DATE(''' || p_from || ''',''YYYYMMDD'')
           AND calday <  TO_DATE(''' || p_to   || ''',''YYYYMMDD'')
         GROUP BY billnum, material
       ),
       bi AS (
         SELECT
           f.billnum,
           DATE_TRUNC(''month'', b.calday)::date AS month_dt,
           f.material,
           b.rpa_sat::numeric / b.qty AS price
         FROM first_bi f
         JOIN std12_116_pr.bills_item b
           ON b.billnum  = f.billnum
          AND b.material = f.material
          AND b.billitem = f.billitem
       ),
       cte2 AS (
         SELECT
           c.plant,
           bi.month_dt,
           SUM(
             CASE
               WHEN p.promo_type = ''002'' THEN bi.price * p.discount / 100
               WHEN p.promo_type = ''001'' THEN p.discount
             END
           )        AS sum_disc,
           COUNT(*) AS cnt_coupons
         FROM std12_116_pr.coupons c
         LEFT JOIN bi
           ON bi.billnum  = c.billnum
          AND bi.material = c.material
         LEFT JOIN std12_116_pr.promotions p
           ON p.promo_id = c.promo_id
         WHERE bi.month_dt >= TO_DATE(''' || p_from || ''',''YYYYMMDD'')
           AND bi.month_dt <  TO_DATE(''' || p_to   || ''',''YYYYMMDD'')
         GROUP BY c.plant, bi.month_dt
       ),
       traf AS (
         SELECT
           plant,
           DATE_TRUNC(''month'', "date")::date AS month_dt,
           SUM(quantity) AS sum_traf
         FROM std12_116_pr.traffic
         WHERE "date" >= TO_DATE(''' || p_from || ''',''YYYYMMDD'')
           AND "date" <  TO_DATE(''' || p_to   || ''',''YYYYMMDD'')
         GROUP BY plant, DATE_TRUNC(''month'', "date")
       )
       SELECT
         c1.plant,
         c1.month_dt                 AS dt,
         c1.revenue,
         COALESCE(c2.sum_disc,0)     AS sum_disc,
         c1.revenue - COALESCE(c2.sum_disc,0) AS sales_wo_discount,
         c1.sum_goods,
         c1.qty_bills,
         COALESCE(t.sum_traf,0)      AS sum_traf,
         COALESCE(c2.cnt_coupons,0)  AS cnt_coupons,
         COALESCE(c2.cnt_coupons::float / NULLIF(c1.sum_goods,0) * 100,0)
                                    AS доляТоваровПоАкции,
         COALESCE(c1.sum_goods::float / NULLIF(c1.qty_bills,0),0)
                                    AS среднКолТовВЧеке,
         COALESCE(c1.qty_bills::float / NULLIF(t.sum_traf,0) * 100,0)
                                    AS КонверсМагазина,
         COALESCE(c1.revenue::float   / NULLIF(c1.qty_bills,0),0)
                                    AS СреднЧек,
         COALESCE(c1.revenue::float   / NULLIF(t.sum_traf,0),0)
                                    AS СрВыручкНаПокупателя
       FROM cte1 c1
  LEFT JOIN cte2 c2 USING (plant, month_dt)
  LEFT JOIN traf   t  USING (plant, month_dt)
      ORDER BY c1.plant, c1.month_dt;';

raise notice 'sql_:  %' , v_sql;
    -- выполнить
    EXECUTE v_sql;

    -- посчитать строки
    EXECUTE 'SELECT COUNT(*) FROM ' || v_full_name INTO v_return;
    RETURN v_return;
END;
$$
EXECUTE ON ANY;


select std12_116_pr.f_load_mart_monthly('20210101', '20210301')


select * from std12_116_pr.sales_report_monthly_20210101_20210301
order by plant, dt
