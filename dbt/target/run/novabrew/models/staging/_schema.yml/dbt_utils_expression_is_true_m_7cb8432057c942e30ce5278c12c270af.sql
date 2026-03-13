
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `novabrew-analytics`.`novabrew_dev_novabrew_marts`.`mart_customer_segments`

where not(r_score r_score between 1 and 4 or r_score is null)


  
  
      
    ) dbt_internal_test