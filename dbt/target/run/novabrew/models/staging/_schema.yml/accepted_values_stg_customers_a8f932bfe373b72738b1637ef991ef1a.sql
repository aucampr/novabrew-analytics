
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        age_band as value_field,
        count(*) as n_records

    from `novabrew-analytics`.`novabrew_dev_novabrew_staging`.`stg_customers`
    group by age_band

)

select *
from all_values
where value_field not in (
    '18-24','25-34','35-44','45-54','55+'
)



  
  
      
    ) dbt_internal_test