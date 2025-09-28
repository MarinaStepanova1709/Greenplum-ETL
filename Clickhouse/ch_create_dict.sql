
DROP DICTIONARY IF EXISTS std12_116_pr.ch_stores;

CREATE DICTIONARY std12_116_pr.ch_stores
(
    plant   String,
    txt String
)
PRIMARY KEY (plant)
SOURCE(PostgreSQL
(
    host   '192.168.214.203'
    port   5432
    user   'std12_116'
    password 'M8oKCODdpha1K4'
    db     'adb'
    schema 'std12_116_pr'
    table  'stores'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 0 MAX 3600);



SELECT    dictGetString('std12_116_pr.ch_stores', 'txt',  'M001' ) AS txtsh_dict




select plant,
	dictGetString('std12_116_pr.ch_stores', 'txt',  region ) AS txtsh_dict

      
SELECT
	plant,
    dictGetString('std12_116_pr.ch_stores', 'txt',  plant ) AS txtsh_dict
FROM std12_116_pr.sales_report_daily_20210101_20210301 rep
LIMIT 100

select
	--dt as "date",
	plant as "Завод",
	dictGetString('std12_116_pr.ch_stores', 'txt',  plant ) AS "Завод(текст)",
	sum(rep.revenue)                                                 AS "Оборот",
    sum (rep.sum_disc)                                                AS "Скидки по купонам",
    sum (rep.sales_wo_discount)                                       AS "Оборот с учетом скидки",
    sum(rep.sum_goods)                                               AS "кол-во проданных товаров",
    sum(rep.qty_bills)                                               AS "Количество чеков",
    sum(rep.sum_traf)                                                AS "Трафик",
    sum(rep.cnt_coupons)                                             AS "Кол-во товаров по акции",
    rep."доляТоваровПоАкции"                                    AS "Доля товаров со скидкой",
    rep."среднКолТовВЧеке"                                      AS "Среднее количество товаров в чеке",
    rep."КонверсМагазина"                                       AS "Коэффициент конверсии магазина, %",
    rep."СреднЧек"                                              AS "Средний чек, руб",
    rep."СрВыручкНаПокупателя"                                  AS "Средняя выручка на одного посетителя, руб"
 from std12_116_pr.sales_report_daily_20210101_20210301 rep
 group by plant










