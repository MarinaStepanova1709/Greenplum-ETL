drop EXTERNAL  table std12_116_pr.coupons_ex

CREATE external TABLE std12_116_pr.coupons_ex (
     plant bpchar(4),
    "date" varchar (10),
    coupon_num bpchar(8),
    promo_id varchar (80),
    article varchar (30),
    billnum int8
)
location ('gpfdist://172.16.128.150:8080/coupons.csv')
ON ALL
FORMAT 'CSV' (delimiter ';' null '' escape '"' quote '"')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 10 ROWS;


select * from std12_116_pr.coupons_ex


drop EXTERNAL  table std12_116_pr.stores_ext;

CREATE external TABLE std12_116_pr.stores_ext (
    plant bpchar(4),
    txt varchar (90)
)
location ('gpfdist://172.16.128.150:8080/stores.csv')
ON ALL
FORMAT 'CSV' (delimiter ';' null '' escape '"' quote '"')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 10 ROWS;


select * from std12_116_pr.stores_ext



drop EXTERNAL  table std12_116_pr.promo_types_ext;

CREATE external TABLE std12_116_pr.promo_types_ext (
    promo_type varchar(10),
    txt_promo varchar (90)
)
location ('gpfdist://172.16.128.150:8080/promo_types.csv')
ON ALL
FORMAT 'CSV' (delimiter ';' null '' escape '"' quote '"')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 10 ROWS;

select * from std12_116_pr.promo_types_ext

drop EXTERNAL  table std12_116_pr.promos_ext;


CREATE external TABLE std12_116_pr.promos_ext(
    promo_id varchar (80),
    promo_name varchar (80),
    promo_type varchar(10),
    article varchar (30),
    discount int
)
location ('gpfdist://172.16.128.150:8080/promotions.csv')
ON ALL
FORMAT 'CSV' (delimiter ';' null '' escape '"' quote '"')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 10 ROWS;

select * from std12_116_pr.promos_ext




