CREATE TABLE std12_116_pr.stores (
    plant VARCHAR(12) NOT NULL,
    txt    VARCHAR(255)
)
DISTRIBUTED REPLICATED;

--drop table std12_116_pr.promo_types

CREATE TABLE std12_116_pr.promo_types (
    promo_type VARCHAR(12) NOT NULL,
    promo_txt      VARCHAR(255)
)
DISTRIBUTED REPLICATED;

drop table std12_116_pr.promotions

CREATE TABLE std12_116_pr.promotions (
	promo_id VARCHAR(32)  NOT null , 
	promo_name    varchar(255),   
	promo_type    VARCHAR(12), 
    article    VARCHAR(30),          
    discount      NUMERIC(10,2) 
)
DISTRIBUTED REPLICATED;





