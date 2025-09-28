drop table std12_116_pr.coupons;

CREATE TABLE std12_116_pr.coupons (
  plant       bpchar(4)    NOT NULL,
  "date"      DATE         NOT NULL,    -- конвертируем из строки
  coupon_num  bpchar(8)    NOT NULL,
  promo_id    VARCHAR(80)  not NULL,
  article     VARCHAR(30)  NULL,
  billnum     BIGINT       NULL
)
with(
	appendonly = true, 
	orientation = column,
	compresstype = zstd,
	compresslevel = 1
)
DISTRIBUTED BY (plant)
partition by range ("date") (
	start (date '2021-01-01') inclusive 
	end (date '2021-02-01') exclusive 
	every (interval '1 month'),
	default partition def
)

ALTER TABLE std12_116_pr.coupons
SET WITH (reorganize = true)
DISTRIBUTED BY (billnum);

select gp_segment_id, count(*)
from std12_116_pr.coupons
group by 1


select gp_segment_id, count(*)
from std12_116_pr.bills_item
group by 1




CREATE EXTERNAL TABLE std12_116_pr.bills_head_ext (
    billnum int8 ,
	plant bpchar(4) ,
	calday date 

	
	drop table std12_116_pr.bills_head
	
CREATE TABLE std12_116_pr.bills_head (
  billnum int8    NOT NULL,
  plant  bpchar(8)    NOT NULL,
  calday date   not NULL
)
with(
	appendonly = true, 
	orientation = column,
	compresstype = zstd,
	compresslevel = 1
)
DISTRIBUTED BY (billnum)
partition by range (calday) (
	start (date '2021-01-01') inclusive 
	end (date '2021-03-01') exclusive 
	every (interval '1 month'),
	default partition def
)


----bill_item
drop table std12_116_pr.bills_item

CREATE TABLE std12_116_pr.bills_item (
  billnum int8    NOT NULL,
  billitem int8    NOT NULL,
  material int8 ,
  qty int8 ,
  netval numeric(17, 2) ,
  tax numeric(17, 2) ,
  rpa_sat numeric(17, 2) ,
  calday date   not NULL
)
with(
	appendonly = true, 
	orientation = column,
	compresstype = zstd,
	compresslevel = 1
)
DISTRIBUTED BY (billnum)
partition by range (calday) (
	start (date '2021-01-01') inclusive 
	end (date '2021-03-01') exclusive 
	every (interval '1 month'),
	default partition def
);


drop table std12_116_pr.traffic;

CREATE TABLE std12_116_pr.traffic (
    plant varchar(12) ,
	"date" date,
	"time" time ,
	date_tr timestamp,
	frame_id varchar(20) ,
	quantity int4 
)
with(
	appendonly = true, 
	orientation = column,
	compresstype = zstd,
	compresslevel = 1
)
DISTRIBUTED BY (frame_id)
partition by range (date_tr) (
	start (date '2021-01-01') inclusive 
	end (date '2021-03-01') exclusive 
	every (interval '1 month'),
	default partition def
);




