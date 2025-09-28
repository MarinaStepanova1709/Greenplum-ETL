drop external table std12_116_pr.traffic;

CREATE EXTERNAL TABLE std12_116_pr.traffic_ext (
    plant bpchar(4) ,
	"date" bpchar(10) ,
	"time" bpchar(6) ,
	frame_id bpchar(10) ,
	quantity int4 
)
LOCATION ('pxf://gp.traffic?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern')
ON ALL
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
ENCODING 'UTF8';





select * from std12_116_pr.traffic_ext

CREATE TABLE gp.bills_head (
	billnum int8 NULL,
	plant bpchar(4) NULL,
	calday date NULL
);

drop external table std12_116_pr.bills_head_ext;

CREATE EXTERNAL TABLE std12_116_pr.bills_head_ext (
    billnum int8 ,
	plant bpchar(4) ,
	calday date 
)
LOCATION ('pxf://gp.bills_head?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern')
ON ALL
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
ENCODING 'UTF8';

select * from std12_116_pr.bills_head_ext




drop external table std12_116_pr.bills_item_ext;

CREATE EXTERNAL TABLE std12_116_pr.bills_item_ext (
    billnum int8 ,
	billitem int8 ,
	material int8 ,
	qty int8 ,
	netval numeric(17, 2) ,
	tax numeric(17, 2) ,
	rpa_sat numeric(17, 2) ,
	calday date  
)
LOCATION ('pxf://gp.bills_item?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern')
ON ALL
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
ENCODING 'UTF8';


select * from std12_116_pr.bills_item_ext;
