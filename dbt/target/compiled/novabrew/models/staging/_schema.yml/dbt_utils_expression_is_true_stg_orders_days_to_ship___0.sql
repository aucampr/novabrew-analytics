



select
    1
from `novabrew-analytics`.`novabrew_dev_novabrew_staging`.`stg_orders`

where not(days_to_ship >= 0)

