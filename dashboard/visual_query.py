QUERIES = {
    'VACCINE_STOCK_COMBINED':"""
        SELECT distributed_janssen, distributed_moderna, distributed_pfizer, distributed_novavax,vsp.high as high,vsp.low as low, vsp.open as "open", vsp.close as "close", vsp.date as "date", vsp.company as company 
        from vaccine_distribution as vd
        INNER JOIN vaccine_stock_price as vsp ON vd.date = vsp.date where vsp.company='{company}';""",

    'TWEET_STOCK_COMBINED' : """
        SELECT retweetcount, likecount, vsp.high as high,vsp.low as low, vsp.open as "open", vsp.close as "close", vsp.date as "date", vsp.company as company 
        FROM vaccine_manuf_tweet as twt 
        INNER JOIN vaccine_stock_price as vsp ON twt.date = vsp.date where vsp.company='{company}' and twt.company = '{company}';""",

    'VACCINE_DISTRIBUTION_AND_STOCK':"""
        SELECT distributed_janssen, distributed_moderna, distributed_pfizer, distributed_novavax,vsp.high as "High",vsp.low as "Low", vsp.open as "Open", vsp.close as "Close",vsp.volume as "Volume", vsp.date as "date", vsp.company as company 
        from vaccine_distribution as vd 
        INNER JOIN vaccine_stock_price as vsp ON vd.date = vsp.date;"""
}