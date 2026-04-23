/* =========================================================
   GOLD LAYER : ORDER + FREIGHT VIEW
   Purpose:
   Combine order rows with matching freight rate, warehouse
   capacity, and warehouse cost information.
   ========================================================= */

create or replace  view gold_order_freight_dim as 
select 
    o.order_id,
    o.prd_id,
    o.customer,
    o.order_dt,
    plant_code,
    wc.daily_capacity,
    o.origin_prt,
    o.destination_port,
    o.carrier,
    f.carrier_type,
    o.Service_level,
    o.unit_qt,
    w.costperunit,
    o.weight,
    f.minm_wgh_qty,
    f.max_wgh_qty,
    f.minimum_cost,
    f.rate,
    (o.unit_qt *   w.costperunit) as warehouse_cost,
    greatest(o.weight * cast(replace(f.rate,'$','') as decimal(10,2)),cast(replace(f.minimum_cost,'$','') as decimal(10,2))) as shipping_cost,
    (o.unit_qt *   w.costperunit) +  greatest(o.weight * cast(replace(f.rate,'$','') as decimal(10,2)),cast(replace(f.minimum_cost,'$','') as decimal(10,2)))as total_cost,
     dense_rank() 
     over (partition by o.order_id order by (o.unit_qt *   w.costperunit) +  greatest(o.weight * cast(replace(f.rate,'$','') as decimal(10,2)),cast(replace(f.minimum_cost,'$','') as decimal(10,2)))asc) as rn

from silver_orders o
join silver_freightrates f
    on o.carrier = f.carrier
   and o.origin_prt = f.orig_port_cd 
   and o.destination_port = f.dest_port_cd 
   and o.weight between f.minm_wgh_qty and f.max_wgh_qty
join silver_whcapacities wc
    on o.plant_code = wc.plant_id
join silver_whcosts w
    on o.plant_code = w.plant_id
order by o.plant_code;



/* =========================================================
   VMI FILTER USING EXISTS / NOT EXISTS
   Purpose:
   Keep rows where:
   1. plant-customer pair is listed in VMI table
   2. or plant is not present in VMI table at all
   ========================================================= */

select *
from gold_order_freight_dim g
where
      exists (
          select 1
          from silver_vmi_customer vmi
          where g.customer = vmi.customers
            and g.plant_code = vmi.plant_code
      )
   or not exists (
          select 1
          from silver_vmi_customer vmi
          where g.plant_code = vmi.plant_code
      )
order by g.plant_code;



/* =========================================================
   VMI FILTER USING LEFT JOINS
   Purpose:
   Same VMI rule as above, but using joins instead of
   correlated subqueries.

   Logic:
   - first left join checks exact plant-customer pair
   - second left join checks whether plant is in VMI list
   - keep row if exact pair matched
     or plant is not restricted
   ========================================================= */

select g.*
from gold_order_freight_dim g
left join silver_vmi_customer v
    on g.plant_code = v.plant_code
   and g.customer = v.customers
left join (
    select distinct plant_code
    from silver_vmi_customer
) rp
    on g.plant_code = rp.plant_code
where v.plant_code is not null
   or rp.plant_code is null
order by g.plant_code;



select
    t.plant_code,
    sum(t.unit_qt) as plant_utilisation,
    max(t.daily_capacity) as daily_capacity
from (
    select
        gd.plant_code,
        gd.unit_qt,
        gd.order_dt,
        wc.daily_capacity,
        lead(gd.order_dt) over (
            partition by gd.plant_code
            order by gd.order_dt
        ) as next_date
    from gold_order_freight_dim gd
    join silver_whcapacities wc
        on gd.plant_code = wc.plant_id
    where gd.rn = 1
) t
where datediff(t.next_date, t.order_dt) = 1
group by t.plant_code;




select
    gd.plant_code,
    gd.order_dt,
    sum(gd.unit_qt) as plant_utilisation,
    max(wc.daily_capacity) as daily_capacity,
    sum(gd.unit_qt) - max(wc.daily_capacity) as excess_load
from gold_order_freight_dim gd
join silver_whcapacities wc
    on gd.plant_code = wc.plant_id
    where rn=1
group by gd.plant_code, gd.order_dt;
