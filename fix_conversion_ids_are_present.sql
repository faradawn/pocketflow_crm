INSERT INTO `identity_resolution`.`customer_to_customer_match`
(
    customer_id,
    og_customer_id,
    unique_customer_key,
    effective_start_date,
    effective_end_date
)
with missing_ids as (
    select
        source_file_name,
        cc.customer_id as old_customer_id
        , cc.conversion_id as conversion_id
        , date(
    COALESCE(DEMAND_DATE_UTC, SHIPPED_DATE_UTC, DEMAND_DATE, SHIPPED_DATE)
) as effective_date
    from `customer`.`conversion` as cc
    left join `identity_resolution`.`customer_to_customer_match` as c2c
        on cc.customer_id = c2c.customer_id
            -- TODO remove non UTC dates once Michigan Lottery gets their dates in order
            and date(
    COALESCE(DEMAND_DATE_UTC, SHIPPED_DATE_UTC, DEMAND_DATE, SHIPPED_DATE)
) between c2c.effective_start_date and coalesce(c2c.effective_end_date, '3000-01-01')
    where c2c.customer_id is null and cc.customer_id is not null
        --Todo figure out how to remove this specific testing conversion id
        -- this conversion id should not be inserting in the unit testing framework to eliminate duplication
        and cc.conversion_id != "test12_order3"
    group by cc.customer_id, effective_date, conversion_id, source_file_name
    order by cc.customer_id, effective_date
)
select missing_ids.old_customer_id, missing_ids.old_customer_id, GENERATE_UUID() AS unique_customer_key, missing_ids.effective_date, missing_ids.effective_date from missing_ids
where source_file_name in (-- Todo: make this a jinja templated test to build out warning functionality
with totals as (
    select count(distinct concat(coalesce(conversion_id, ''), coalesce(customer_id, ''), date(
DEMAND_DATE_UTC
))) as total_count
    from `customer`.`conversion`
    where coalesce(customer_id, '') != ''
)
, missing_ids as (
    select
    source_file_name,
        cc.customer_id as old_customer_id
        , cc.conversion_id as conversion_id
        , date(
DEMAND_DATE_UTC
) as effective_date
    from `customer`.`conversion` as cc
    left join `identity_resolution`.`customer_to_customer_match` as c2c
        on cc.customer_id = c2c.customer_id
            -- TODO remove non UTC dates once Michigan Lottery gets their dates in order
            and date(
DEMAND_DATE_UTC
) between c2c.effective_start_date and coalesce(c2c.effective_end_date, '3000-01-01')
    where c2c.customer_id is null and cc.customer_id is not null
        --Todo figure out how to remove this specific testing conversion id
        -- this conversion id should not be inserting in the unit testing framework to eliminate duplication
        and cc.conversion_id != "test12_order3"
    group by cc.customer_id, effective_date, conversion_id, source_file_name
    order by cc.customer_id, effective_date
)
select source_file_name
from missing_ids
group by 1)