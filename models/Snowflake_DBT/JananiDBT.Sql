select PRODUCT_ID, SKU, PRODUCT_NAME , Case when Category = 'Category 1' then 'Electronics' when category = 'Category 2' then 'Electrical'
else category end as CATEGORY
from SUPABASE.POSTGRES.PRODUCT
order by PRODUCT_ID