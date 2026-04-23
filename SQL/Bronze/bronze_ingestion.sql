
TRUNCATE TABLE bronze_orders;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.6/Uploads/supply chain/orders.csv'
INTO TABLE bronze_orders
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
    @order_id,
    @order_dt,
    @origin_prt,
    @carrier,
    @tpt,
    @service_level,
    @ship_ahead_day_ct,
    @ship_ahead_late_ct,
    @customer,
    @product_id,
    @plant_code,
    @destination_port,
    @unit_qt,
    @weight
)
SET
    order_id            = NULLIF(TRIM(@order_id), ''),
    order_dt            = str_to_date(NULLIF(TRIM(@order_dt), ''),'%d-%m-%Y'),
    origin_prt          = NULLIF(TRIM(@origin_prt), ''),
    carrier             = NULLIF(TRIM(@carrier), ''),
    TPT                 = NULLIF(TRIM(@tpt), ''),
    Service_level       = NULLIF(TRIM(@service_level), ''),
    ship_ahead_day_ct   = NULLIF(TRIM(@ship_ahead_day_ct), ''),
    ship_ahead_late_ct  = NULLIF(TRIM(@ship_ahead_late_ct), ''),
    customer            = NULLIF(TRIM(@customer), ''),
    product_id          = NULLIF(TRIM(@product_id), ''),
    plant_code          = NULLIF(TRIM(@plant_code), ''),
    destination_port    = NULLIF(TRIM(@destination_port), ''),
    unit_qt             = NULLIF(TRIM(@unit_qt), ''),
    weight              = NULLIF(TRIM(@weight), '');
    
    
    
   truncate table  bronze_freightrates;
   load data infile 'C:/ProgramData/MySQL/MySQL Server 9.6/Uploads/supply chain/freightrates.csv'
   into table bronze_freightrates
   fields terminated by ','
   lines terminated by '\r\n'
   ignore 1 rows 
   (@carrier,@orig_port_cd,@dest_port_cd,@minm_wgh_qty,@max_wgh_qty,@svc_cd,@minimum_cost,@rate,@mode_dsc,@tpt_day_cnt,@carrier_type)
   set 
   carrier=NULLIF(TRIM(@carrier),''),
   orig_port_cd=NULLIF(TRIM(@orig_port_cd),''),
   dest_port_cd=NULLIF(TRIM(@dest_port_cd),''),
   minm_wgh_qty=NULLIF(TRIM(@minm_wgh_qty),''),
   max_wgh_qty=NULLIF(TRIM(@max_wgh_qty),''),
   svc_cd=NULLIF(TRIM(@svc_cd),''),
   minimum_cost=NULLIF(TRIM(@minimum_cost),''),
   rate=NULLIF(TRIM(@rate),''),
   mode_dsc=NULLIF(TRIM(@mode_dsc),''),
   tpt_day_cnt=NULLIF(TRIM(@tpt_day_cnt),''),
   carrier_type=NULLIF(TRIM(@carrier_type),'');
   
       
   truncate table  bronze_plantports;
   load data infile 'C:/ProgramData/MySQL/MySQL Server 9.6/Uploads/supply chain/plantports.csv'
   into table bronze_plantports
   fields terminated by ','
   lines terminated by '\r\n'
   ignore 1 rows 
   (@plant_code,@port_number)
   set  
    plant_code=NULLIF(TRIM(@plant_code),''),
    port_number=NULLIF(TRIM(@port_number),'');
    
      truncate table  bronze_plant_prd;
      load data infile 'C:/ProgramData/MySQL/MySQL Server 9.6/Uploads/supply chain/Productsperplant.csv'
     into table bronze_plant_prd
     fields terminated by ','
     lines terminated by '\r\n'
     ignore 1 rows 
     (@plant_code,@prd_id)
     set 
     plant_code=nullif(trim(@plant_code),''),
     prd_id=nullif(trim(@prd_id),'');
     
     
     truncate table  bronze_vmi_customer;
      load data infile 'C:/ProgramData/MySQL/MySQL Server 9.6/Uploads/supply chain/vmicustomers.csv'
     into table bronze_vmi_customer
     fields terminated by ','
     lines terminated by '\r\n'
     ignore 1 rows 
     (@plant_code,@customers)
     set 
     plant_code=nullif(trim(@plant_code),''),
     customers=nullif(trim(@customers),'');
     
     
          
     truncate table  bronze_whcapacities;
      load data infile 'C:/ProgramData/MySQL/MySQL Server 9.6/Uploads/supply chain/whcapacities.csv'
     into table bronze_whcapacities
     fields terminated by ','
     lines terminated by '\r\n'
     ignore 1 rows 
     (@plant_id,@daily_capacity)
     set 
     plant_id=nullif(trim(@plant_id),''),
    daily_capacity=nullif(trim(@daily_capacity),'');
    


              
     truncate table  bronze_whcosts;
      load data infile 'C:/ProgramData/MySQL/MySQL Server 9.6/Uploads/supply chain/whcosts.csv'
     into table  bronze_whcosts
     fields terminated by ','
     lines terminated by '\r\n'
     ignore 1 rows 
     (@plant_id,@costperunit)
     set 
     plant_id=nullif(trim(@plant_id),''),
    costperunit=nullif(trim(@costperunit),'');
    
     
