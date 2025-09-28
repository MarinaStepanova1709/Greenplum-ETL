Create database std12_116_pr;

DROP TABLE IF EXISTS std12_116_pr.sales_report_daily_20210101_20210301;

CREATE TABLE std12_116_pr.sales_report_daily_20210101_20210301  (
	plant String,
	dt date,
	revenue Decimal(18,2), 
	sum_disc Decimal(18,2),
	sales_wo_discount Decimal(18,2),
	sum_goods Int,
	qty_bills Int, 
	sum_traf Int,
	cnt_coupons Int	  ,
	"доляТоваровПоАкции" Decimal(18,2),
	"среднКолТовВЧеке" Decimal(18,2),
	"КонверсМагазина" Decimal(18,2),
	"СреднЧек"  Decimal(19,4),
	"СрВыручкНаПокупателя" Decimal(19,4)
)
Engine = PostgreSQL('192.168.214.203:5432', 'adb', 'sales_report_daily_20210101_20210301', 'std12_116', '***', 'std12_116_pr');
order by  dt


select * from std12_116_pr.sales_report_daily_20210101_20210301 


CREATE TABLE std12_116_pr.sales_report_monthly_20210101_20210301  (
	plant String,
	dt date,
	revenue Decimal(18,2), 
	sum_disc Decimal(18,2),
	sales_wo_discount Decimal(18,2),
	sum_goods Int,
	qty_bills Int, 
	sum_traf Int,
	cnt_coupons Int	  ,
	"доляТоваровПоАкции" Decimal(18,2),
	"среднКолТовВЧеке" Decimal(18,2),
	"КонверсМагазина" Decimal(18,2),
	"СреднЧек"  Decimal(19,4),
	"СрВыручкНаПокупателя" Decimal(19,4)
)
Engine = PostgreSQL('192.168.214.203:5432', 'adb', 'sales_report_monthly_20210101_20210301', 'std12_116', '***', 'std12_116_pr');
order by  dt



