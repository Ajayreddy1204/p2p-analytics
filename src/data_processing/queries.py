"""SQL queries for Redshift data warehouse."""

KPI_QUERIES = {
    'revenue_metrics': """
        SELECT 
            COALESCE(SUM(total_revenue), 0) as total_revenue,
            COALESCE(AVG(total_revenue), 0) as avg_revenue,
            COALESCE(AVG(revenue_growth_mom_percent), 0) as revenue_growth_pct,
            COALESCE(AVG(gross_profit_margin_pct), 0) as margin_pct
        FROM dealer.information_mart.vw_gross_profit_margin
        WHERE {where_clause}
    """,

    'operational_metrics': """
        SELECT 
            COALESCE(AVG(avg_turnaround_hours), 0) as avg_repair_tat_hours,
            COALESCE(AVG(avg_order_lead_time_days), 0) as avg_lead_time_days,
            COALESCE(AVG(stock_availability_pct), 0) as stock_availability_pct,
            COALESCE(AVG(backorder_incidence_pct), 0) as backorder_incidence_pct
        FROM dealer.information_mart.vw_operational_metrics
        WHERE {where_clause}
    """,

    'financial_metrics': """
        SELECT 
            COALESCE(AVG(ccc_days), 0) as ccc_days,
            COALESCE(AVG(dso_days), 0) as dso_days,
            COALESCE(AVG(dio_days), 0) as dio_days,
            COALESCE(AVG(dpo_days), 0) as dpo_days
        FROM dealer.information_mart.vw_cash_conversion_cycle
        WHERE {where_clause}
    """,

    'dealer_health_scores': """
        SELECT 
            m.dealer_name,
            COALESCE(AVG(m.gross_profit_margin_pct), 0) as margin_pct,
            COALESCE(AVG(t.avg_turnaround_hours), 0) as tat_hours,
            COALESCE(AVG(l.avg_order_lead_time_days), 0) as lead_days,
            COALESCE(AVG(s.stock_availability_pct), 0) as stock_pct,
            CASE 
                WHEN AVG(m.gross_profit_margin_pct) >= 30 THEN 100
                WHEN AVG(m.gross_profit_margin_pct) >= 20 THEN 75
                WHEN AVG(m.gross_profit_margin_pct) >= 10 THEN 50
                ELSE 25
            END as margin_score,
            CASE 
                WHEN AVG(t.avg_turnaround_hours) <= 48 THEN 100
                WHEN AVG(t.avg_turnaround_hours) <= 72 THEN 75
                WHEN AVG(t.avg_turnaround_hours) <= 96 THEN 50
                ELSE 25
            END as tat_score,
            CASE 
                WHEN AVG(l.avg_order_lead_time_days) <= 7 THEN 100
                WHEN AVG(l.avg_order_lead_time_days) <= 14 THEN 75
                WHEN AVG(l.avg_order_lead_time_days) <= 21 THEN 50
                ELSE 25
            END as lead_score,
            CASE 
                WHEN AVG(s.stock_availability_pct) >= 90 THEN 100
                WHEN AVG(s.stock_availability_pct) >= 75 THEN 75
                WHEN AVG(s.stock_availability_pct) >= 60 THEN 50
                ELSE 25
            END as stock_score
        FROM dealer.information_mart.vw_gross_profit_margin m
        LEFT JOIN dealer.information_mart.vw_average_repair_turnaround_time t 
            ON m.dealer_name = t.dealer_name
        LEFT JOIN dealer.information_mart.vw_order_lead_time l 
            ON m.dealer_name = l.dealer_name
        LEFT JOIN dealer.information_mart.vw_stock_availability_dealer s 
            ON m.dealer_name = s.dealer_name
        WHERE {where_clause}
        GROUP BY m.dealer_name
    """,

    'dealer_list': """
        SELECT DISTINCT dealer_name
        FROM dealer.information_mart.vw_cash_conversion_cycle
        WHERE dealer_name IS NOT NULL
        ORDER BY dealer_name
    """,

    'product_list': """
        SELECT DISTINCT product_category
        FROM dealer.information_mart.vw_sales_per_product_category
        WHERE product_category IS NOT NULL
        ORDER BY product_category
    """,

    'revenue_trend': """
        SELECT 
            dealer_name,
            period_year,
            period_month,
            total_revenue
        FROM dealer.information_mart.vw_gross_profit_margin
        WHERE {where_clause}
        ORDER BY period_year, period_month
    """,

    'sales_by_category': """
        SELECT 
            product_category,
            SUM(total_revenue) as total_revenue,
            SUM(total_quantity) as total_quantity
        FROM dealer.information_mart.vw_sales_per_product_category
        WHERE {where_clause}
        GROUP BY product_category
        ORDER BY total_revenue DESC
        LIMIT 10
    """
}