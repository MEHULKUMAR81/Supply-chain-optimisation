drop table if exists  silver_orders;
create table silver_orders 
(
order_id int, 
order_dt date,
origin_prt  varchar(50),
carrier  varchar(50),
TPT varchar(50),
Service_level  varchar(50),
ship_ahead_day_ct  varchar(50),
ship_ahead_late_ct  varchar(50),
customer varchar(50),
prd_id  varchar(50),
plant_code  varchar(50),
destination_port  varchar (50) ,
unit_qt  int,
weight int
);
drop table if exists  silver_freightrates ;
create table silver_freightrates
(
 carrier  varchar(50), 
orig_port_cd varchar(50),
dest_port_cd varchar(50),
minm_wgh_qty decimal(10,2),
max_wgh_qty decimal(10,2),
svc_cd  varchar(50),
minimum_cost varchar(50),
rate  varchar(50),
mode_dsc varchar(50),
tpt_day_cnt  int,
carrier_type varchar(50)
);

drop table if exists  silver_plantports ;
create table silver_plantports
(
plant_code varchar(50),
Port_number varchar(50)
)
;

drop table if exists silver_plant_prd ;
create table silver_plant_prd (
plant_code varchar(50),
prd_id     int );


drop table if exists silver_Vmi_customer;
create table silver_Vmi_customer(
plant_code varchar(50),
customers varchar(50))
;

drop table if exists silver_whcapacities;
create table silver_whcapacities(
plant_id varchar(50),
daily_capacity varchar(50));

drop table if exists silver_whcosts;
create table silver_whcosts(
plant_id varchar(50),
costperunit decimal(10,2))  




----------------------------------------------------------------------------------
----------------------------------------------------------------------------------

/* =========================================================
   SILVER LAYER VALIDATION + LOAD SCRIPT
   Purpose:
   - Check basic data quality issues in bronze tables
   - Filter or move cleaned data into silver tables
   - Preserve business rules before downstream analysis
   ========================================================= */


/* =========================================================
   1. ORDERS
   ========================================================= */

/* Check rows where ordered quantity is missing */
select*
from bronze_orders
where order_id='1447164686'

/* Check duplicate order IDs */
select order_id , count(*)
from bronze_orders
group by order_id
having count(*) >1;

/* Reload silver_orders from cleaned bronze_orders
   Exclude V44_3 carrier and CRF service level
   because these are customer-managed / discontinued cases */
truncate table silver_orders;

insert into silver_orders(
order_id,order_dt,origin_prt,carrier,TPT,Service_level,ship_ahead_day_ct,ship_ahead_late_ct,customer,prd_id,plant_code,destination_port,unit_qt,weight)
select 
order_id, order_dt,
origin_prt,carrier,TPT,Service_level,ship_ahead_day_ct,ship_ahead_late_ct,customer,product_id,plant_code,destination_port,unit_qt,weight
from bronze_orders
where carrier != 'V44_3' and Service_level != 'CRF' 



/* =========================================================
   2. FREIGHT RATES
   ========================================================= */

/* Check rows where max weight is less than min weight */
select*
from bronze_freightrates
where max_wgh_qty < minm_wgh_qty;

/* Check rows where origin and destination ports are the same */
select*
from bronze_freightrates
where orig_port_cd = dest_port_cd;

/* Check rows where min and max weight slabs are equal */
select*
from bronze_freightrates
where minm_wgh_qty=max_wgh_qty;

/* Reload silver_freightrates
   Keep only rows where origin and destination are different */
truncate table silver_freightrates;

insert into silver_freightrates(carrier,orig_port_cd,dest_port_cd,minm_wgh_qty,max_wgh_qty,svc_cd,minimum_cost,rate,mode_dsc,tpt_day_cnt,carrier_type)
select 
carrier,orig_port_cd,dest_port_cd,minm_wgh_qty,max_wgh_qty,svc_cd,minimum_cost,rate,mode_dsc,tpt_day_cnt,carrier_type
from bronze_freightrates
where orig_port_cd != dest_port_cd;



/* =========================================================
   3. PLANT - PRODUCT MAPPING
   ========================================================= */

/* Reload silver_plant_prd from bronze_plant_prd */
truncate table silver_plant_prd;

insert into silver_plant_prd(plant_code,prd_id)
select 
plant_code,prd_id
from  bronze_plant_prd;



/* =========================================================
   4. PLANT - PORT MAPPING
   ========================================================= */

/* Reload silver_plantports from bronze_plantports */
truncate table silver_plantports;

insert into silver_plantports(plant_code,port_number)
select 
plant_code,port_number
from bronze_plantports;



/* =========================================================
   5. VMI CUSTOMERS
   ========================================================= */

/* Check rows where customer value is missing */
select * from bronze_vmi_customer
where customers is null;

/* Reload silver_vmi_customer from bronze_vmi_customer */
truncate table silver_vmi_customer;

insert into  silver_vmi_customer(plant_code,customers)
select 
plant_code,customers
from bronze_vmi_customer;



/* =========================================================
   6. WAREHOUSE CAPACITIES
   ========================================================= */

/* Check plants with zero daily capacity */
select* from bronze_whcapacities
where daily_capacity =0;

/* Reload silver_whcapacities from bronze_whcapacities */
truncate table silver_whcapacities;

insert into  silver_whcapacities(plant_id,daily_capacity)
select 
plant_id,daily_capacity
from bronze_whcapacities;



/* =========================================================
   7. WAREHOUSE COSTS
   ========================================================= */

/* Reload silver_whcosts from bronze_whcosts */
truncate table silver_whcosts;

insert into  silver_whcosts(plant_id,costperunit)
select 
plant_id,costperunit
from bronze_whcosts;






