select * from {{ ref('mart_monthly_funnel') }} where conversion_pct < 0 or conversion_pct > 100
