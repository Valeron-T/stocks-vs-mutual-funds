SELECT date, nifty50_nav, midcap_nav, smallcap_nav, stock_price
	FROM public.prices;
	
-- SELECT * from public.prices where date in ('2006-04-03 00:00:00','2023-08-24 00:00:00');

DO $$
DECLARE
    start_date date := '2011-12-12';
    end_date date := '2023-08-24';
    percentage_increase_nifty50 numeric;
    percentage_increase_midcap numeric;
    percentage_increase_smallcap numeric;
    percentage_increase_stock numeric;
BEGIN
    -- Execute the dynamic SQL query
    EXECUTE '
        SELECT 
            ((nifty50_nav_end - nifty50_nav_start) / nifty50_nav_start) * 100 AS percentage_increase_nifty50,
            ((midcap_nav_end - midcap_nav_start) / midcap_nav_start) * 100 AS percentage_increase_midcap,
            ((smallcap_nav_end - smallcap_nav_start) / smallcap_nav_start) * 100 AS percentage_increase_smallcap,
            ((stock_price_end - stock_price_start) / stock_price_start) * 100 AS percentage_increase_stock
        FROM (
            SELECT 
                nifty50_nav AS nifty50_nav_start,
                midcap_nav AS midcap_nav_start,
                smallcap_nav AS smallcap_nav_start,
                stock_price AS stock_price_start
            FROM public.prices
            WHERE date = $1
        ) AS subquery_start,
        (
            SELECT 
                nifty50_nav AS nifty50_nav_end,
                midcap_nav AS midcap_nav_end,
                smallcap_nav AS smallcap_nav_end,
                stock_price AS stock_price_end
            FROM public.prices
            WHERE date = $2
        ) AS subquery_end'
    INTO percentage_increase_nifty50, percentage_increase_midcap, percentage_increase_smallcap, percentage_increase_stock
    USING start_date, end_date;
    
    -- Print the result
	RAISE NOTICE '------------  MUTUAL FUNDS --------------';
    RAISE NOTICE 'Percentage Increase (Nifty50 - No/Low Volatility): %', percentage_increase_nifty50;
    RAISE NOTICE 'Percentage Increase (Midcap - Med Volatility): %', percentage_increase_midcap;
    RAISE NOTICE 'Percentage Increase (Smallcap - High Volatility): %', percentage_increase_smallcap;
	RAISE NOTICE '-----------  SELECTED STOCK -------------';
    RAISE NOTICE 'Percentage Increase (Stock Price): %', percentage_increase_stock;
END $$;
