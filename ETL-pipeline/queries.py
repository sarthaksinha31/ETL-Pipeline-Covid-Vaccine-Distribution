query={
    'CREATE_VACCINE_DISTRIBUTION_TABLE':"""
        CREATE TABLE IF NOT EXISTS vaccine_distribution (
        id serial primary key,
        date date NOT NULL,
        distributed_janssen integer NOT NULL,
        distributed_moderna integer NOT NULL,
        distributed_pfizer integer NOT NULL,
        distributed_novavax integer NOT NULL
        );""",

    'CREATE_STOCK_TABLE':"""
        CREATE TABLE IF NOT EXISTS vaccine_stock_price (
        id serial primary key,
        date date NOT NULL,
        company character varying(100) NOT NULL,
        high float NOT NULL,
        open float NOT NULL,
        close float NOT NULL,
        low float NOT NULL,
        volume float NOT NULL
        );""",

    'CREATE_TWITTER_TABLE':"""
        CREATE TABLE IF NOT EXISTS vaccine_manuf_tweet (
        id serial primary key,
        date date NOT NULL,
        company character varying(100) NOT NULL,
        likecount integer NOT NULL,
        retweetcount integer NOT NULL
        );"""
}