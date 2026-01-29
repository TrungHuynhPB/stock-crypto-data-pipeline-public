CREATE TABLE "model.market_data_pipeline.dim_assets" (
  "asset_symbol" text,
  "asset_type" text,
  "asset_name" text,
  "asset_category" text,
  "asset_class" text,
  "market_cap_category" text,
  "sector" text,
  "is_tradeable" boolean,
  "created_at" timestamp_ltz,
  "asset_key" text,
  "volatility_category" text,
  "liquidity_category" text
);

CREATE TABLE "model.market_data_pipeline.dim_crypto" (
  "symbol" text,
  "base_currency" text,
  "market_cap" number,
  "source" text,
  "load_timestamp" timestamp_ntz
);

CREATE TABLE "model.market_data_pipeline.dim_customers_scd2" (
  "customer_id" text,
  "first_name" text,
  "last_name" text,
  "full_name" text,
  "email" text,
  "gender" text,
  "age_group" text,
  "country" text,
  "customer_tier" text,
  "risk_tolerance" text,
  "registration_date" text,
  "customer_tenure_days" number,
  "valid_from" timestamp_ntz,
  "load_timestamp" timestamp_ntz,
  "tier_changed" boolean,
  "risk_tolerance_changed" boolean,
  "country_changed" boolean,
  "valid_to" timestamp_ntz,
  "change_type" text,
  "is_current" boolean,
  "customer_key" text,
  "change_description" text
);

CREATE TABLE "model.market_data_pipeline.dim_stocks" (
  "ticker" text,
  "company_name" text,
  "sector" text,
  "industry" text,
  "market_cap" number,
  "pe_ratio" number,
  "week_52_high" number,
  "week_52_low" number,
  "avg_volume" number,
  "load_timestamp" timestamp_ntz
);

CREATE TABLE "model.market_data_pipeline.fact_customer_portfolio" (
  "customer_id" text,
  "asset_symbol" text,
  "asset_type" text,
  "current_quantity" float,
  "total_invested_amount" float,
  "total_transactions" number,
  "first_transaction_date" timestamp_ntz,
  "last_transaction_date" timestamp_ntz,
  "last_activity_date" text,
  "current_market_value" float,
  "unrealized_pnl" float,
  "unrealized_pnl_percentage" float,
  "position_type" text,
  "position_size_category" text,
  "portfolio_key" text,
  "asset_name" text,
  "asset_category" text,
  "asset_class" text,
  "market_cap_category" text,
  "sector" text,
  "volatility_category" text,
  "liquidity_category" text,
  "customer_tier" text,
  "risk_tolerance" text,
  "gender" text,
  "age_group" text,
  "country" text,
  "customer_tenure_days" number,
  "latest_news_url" text,
  "latest_news_title" text,
  "position_value_category" text,
  "days_since_last_activity" number
);

CREATE TABLE "model.market_data_pipeline.fact_daily_crypto_prices" (
  "event_timestamp" timestamp_ntz,
  "symbol" text,
  "open_price" number,
  "high_price" number,
  "low_price" number,
  "close_price" number,
  "volume" number,
  "quote_volume" number
);

CREATE TABLE "model.market_data_pipeline.fact_daily_stock_prices" (
  "date" date,
  "ticker" text,
  "open_price" number,
  "high_price" number,
  "low_price" number,
  "close_price" number,
  "adj_close_price" number,
  "volume" number,
  "dividends" number,
  "stock_splits" number
);

CREATE TABLE "model.market_data_pipeline.fact_daily_trading_summary" (
  "daily_summary_key" text,
  "data_date" date,
  "customer_id" text,
  "asset_symbol" text,
  "asset_type" text,
  "total_buy_quantity" float,
  "total_buy_amount" float,
  "total_buy_fees" float,
  "buy_transaction_count" number,
  "total_sell_quantity" float,
  "total_sell_amount" float,
  "total_sell_fees" float,
  "sell_transaction_count" number,
  "total_quantity" float,
  "total_amount" float,
  "total_fees" float,
  "total_transaction_count" number,
  "avg_price" float,
  "min_price" float,
  "max_price" float,
  "first_transaction_time" number,
  "last_transaction_time" number,
  "net_quantity_change" float,
  "net_amount_change" float,
  "activity_level" text,
  "volume_category" text,
  "daily_price_volatility" float,
  "fee_efficiency_ratio" float,
  "asset_name" text,
  "asset_category" text,
  "asset_class" text,
  "market_cap_category" text,
  "sector" text,
  "volatility_category" text,
  "liquidity_category" text,
  "customer_tier" text,
  "risk_tolerance" text,
  "gender" text,
  "age_group" text,
  "country" text,
  "customer_tenure_days" number,
  "trading_year" number,
  "trading_month" number,
  "trading_day" number,
  "trading_day_of_week" number,
  "trading_week" number,
  "trading_quarter" number
);

CREATE TABLE "model.market_data_pipeline.fact_news" (
  "date" timestamp_ntz,
  "ticker" text,
  "asset_type" text,
  "url" text,
  "title" text,
  "description" text,
  "image" text,
  "category" text
);

CREATE TABLE "model.market_data_pipeline.fact_transactions" (
  "transaction_id" text,
  "customer_id" text,
  "asset_type" text,
  "asset_symbol" text,
  "transaction_type" text,
  "quantity" float,
  "price_per_unit" float,
  "transaction_amount" float,
  "fee_amount" float,
  "transaction_timestamp" timestamp_ntz,
  "data_date" text,
  "net_transaction_amount" float,
  "net_quantity" float,
  "customer_tier" text,
  "risk_tolerance" text,
  "gender" text,
  "age_group" text,
  "country" text,
  "customer_tenure_days" number,
  "asset_name" text,
  "asset_category" text,
  "asset_class" text,
  "market_cap_category" text,
  "sector" text,
  "volatility_category" text,
  "liquidity_category" text,
  "is_invalid_amount" boolean,
  "is_invalid_quantity" boolean,
  "load_timestamp" timestamp_ntz,
  "data_source" text,
  "transaction_year" number,
  "transaction_month" number,
  "transaction_day" number,
  "transaction_hour" number,
  "transaction_day_of_week" number,
  "total_cost" float,
  "fee_percentage" float,
  "transaction_key" text,
  "customer_key" text,
  "asset_key" text,
  "transaction_size_category" text,
  "transaction_category" text,
  "trading_session" text,
  "trading_day_type" text
);

CREATE TABLE "model.market_data_pipeline.stg_crypto" (
  "unknown" unknown
);

CREATE TABLE "model.market_data_pipeline.stg_customers" (
  "customer_id" text,
  "first_name" text,
  "last_name" text,
  "email" text,
  "gender" text,
  "age_group" text,
  "country" text,
  "registration_date" text,
  "customer_tier" text,
  "risk_tolerance" text,
  "load_timestamp" number,
  "full_name" text,
  "customer_tenure_days" number,
  "is_invalid_email" boolean,
  "is_invalid_registration_date" boolean,
  "rn" number
);

CREATE TABLE "model.market_data_pipeline.stg_news" (
  "unknown" unknown
);

CREATE TABLE "model.market_data_pipeline.stg_stocks" (
  "unknown" unknown
);

CREATE TABLE "model.market_data_pipeline.stg_transactions" (
  "asset_symbol" text,
  "asset_type" text,
  "customer_id" text,
  "customer_risk_tolerance" text,
  "customer_tier" text,
  "data_date" text,
  "data_source" text,
  "fee_amount" float,
  "is_invalid_amount" boolean,
  "is_invalid_quantity" boolean,
  "load_timestamp" number,
  "net_quantity" float,
  "net_transaction_amount" float,
  "price_per_unit" float,
  "quantity" float,
  "transaction_amount" float,
  "transaction_id" text,
  "transaction_timestamp" number,
  "transaction_type" text
);

COMMENT ON TABLE "model.market_data_pipeline.dim_assets" IS 'Asset dimension table';

COMMENT ON COLUMN "model.market_data_pipeline.dim_assets"."asset_symbol" IS 'Asset symbol (e.g., AAPL, BTC)';

COMMENT ON COLUMN "model.market_data_pipeline.dim_assets"."asset_type" IS 'Type of asset (STOCK or CRYPTO)';

COMMENT ON COLUMN "model.market_data_pipeline.dim_assets"."asset_name" IS 'Full name of the asset';

COMMENT ON COLUMN "model.market_data_pipeline.dim_assets"."asset_category" IS 'Category of the asset';

COMMENT ON COLUMN "model.market_data_pipeline.dim_assets"."asset_class" IS 'Class of the asset';

COMMENT ON COLUMN "model.market_data_pipeline.dim_assets"."market_cap_category" IS 'Market cap category';

COMMENT ON COLUMN "model.market_data_pipeline.dim_assets"."sector" IS 'Sector of the asset';

COMMENT ON COLUMN "model.market_data_pipeline.dim_assets"."asset_key" IS 'Surrogate key for asset dimension';

COMMENT ON COLUMN "model.market_data_pipeline.dim_assets"."volatility_category" IS 'Volatility category';

COMMENT ON COLUMN "model.market_data_pipeline.dim_assets"."liquidity_category" IS 'Liquidity category';

COMMENT ON TABLE "model.market_data_pipeline.dim_crypto" IS 'Dimension table for crypto assets';

COMMENT ON COLUMN "model.market_data_pipeline.dim_crypto"."symbol" IS 'Currency Pair';

COMMENT ON COLUMN "model.market_data_pipeline.dim_crypto"."base_currency" IS 'Base currency';

COMMENT ON COLUMN "model.market_data_pipeline.dim_crypto"."market_cap" IS 'Market capitalization in USD';

COMMENT ON COLUMN "model.market_data_pipeline.dim_crypto"."source" IS 'Source of exchanges API pulling from';

COMMENT ON TABLE "model.market_data_pipeline.dim_customers_scd2" IS 'Customer dimension table with SCD Type 2';

COMMENT ON COLUMN "model.market_data_pipeline.dim_customers_scd2"."customer_id" IS 'Business key for customer';

COMMENT ON COLUMN "model.market_data_pipeline.dim_customers_scd2"."valid_from" IS 'Start date for this customer record version';

COMMENT ON COLUMN "model.market_data_pipeline.dim_customers_scd2"."valid_to" IS 'End date for this customer record version';

COMMENT ON COLUMN "model.market_data_pipeline.dim_customers_scd2"."change_type" IS 'Type of change (NEW or CHANGED)';

COMMENT ON COLUMN "model.market_data_pipeline.dim_customers_scd2"."is_current" IS 'Flag indicating if this is the current customer record';

COMMENT ON COLUMN "model.market_data_pipeline.dim_customers_scd2"."customer_key" IS 'Surrogate key for customer dimension';

COMMENT ON TABLE "model.market_data_pipeline.dim_stocks" IS 'Dimension table for stocks';

COMMENT ON COLUMN "model.market_data_pipeline.dim_stocks"."ticker" IS 'Stock ticker symbol';

COMMENT ON COLUMN "model.market_data_pipeline.dim_stocks"."company_name" IS 'Name of the company';

COMMENT ON COLUMN "model.market_data_pipeline.dim_stocks"."sector" IS 'Sector classification';

COMMENT ON COLUMN "model.market_data_pipeline.dim_stocks"."industry" IS 'Industry classification';

COMMENT ON COLUMN "model.market_data_pipeline.dim_stocks"."market_cap" IS 'Market capitalization value';

COMMENT ON COLUMN "model.market_data_pipeline.dim_stocks"."pe_ratio" IS 'Price-to-Earnings ratio';

COMMENT ON COLUMN "model.market_data_pipeline.dim_stocks"."week_52_high" IS 'Highest stock price in the past 52 weeks';

COMMENT ON COLUMN "model.market_data_pipeline.dim_stocks"."week_52_low" IS 'Lowest stock price in the past 52 weeks';

COMMENT ON COLUMN "model.market_data_pipeline.dim_stocks"."avg_volume" IS 'Average daily trading volume';

COMMENT ON TABLE "model.market_data_pipeline.fact_customer_portfolio" IS 'Fact table for customer portfolio positions';

COMMENT ON COLUMN "model.market_data_pipeline.fact_customer_portfolio"."customer_id" IS 'Customer identifier';

COMMENT ON COLUMN "model.market_data_pipeline.fact_customer_portfolio"."asset_symbol" IS 'Asset symbol';

COMMENT ON COLUMN "model.market_data_pipeline.fact_customer_portfolio"."current_quantity" IS 'Current quantity held';

COMMENT ON COLUMN "model.market_data_pipeline.fact_customer_portfolio"."current_market_value" IS 'Current market value of position';

COMMENT ON COLUMN "model.market_data_pipeline.fact_customer_portfolio"."unrealized_pnl" IS 'Unrealized profit/loss';

COMMENT ON COLUMN "model.market_data_pipeline.fact_customer_portfolio"."position_type" IS 'Type of position (LONG, SHORT, CLOSED)';

COMMENT ON COLUMN "model.market_data_pipeline.fact_customer_portfolio"."portfolio_key" IS 'Surrogate key for portfolio position';

COMMENT ON TABLE "model.market_data_pipeline.fact_daily_crypto_prices" IS 'Fact table for daily crypto prices';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_crypto_prices"."event_timestamp" IS 'Timestamp of the crypto price record';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_crypto_prices"."symbol" IS 'Currency Pair';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_crypto_prices"."open_price" IS 'Opening price of quote';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_crypto_prices"."high_price" IS 'Highest price of quote';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_crypto_prices"."low_price" IS 'Lowest price of quote';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_crypto_prices"."close_price" IS 'Price of cryptocurreny to USD';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_crypto_prices"."volume" IS 'Volume of exchange';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_crypto_prices"."quote_volume" IS 'Quote volume';

COMMENT ON TABLE "model.market_data_pipeline.fact_daily_stock_prices" IS 'Fact table for daily stock prices';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_stock_prices"."date" IS 'Date of the stock price record';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_stock_prices"."ticker" IS 'Stock ticker symbol';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_stock_prices"."open_price" IS 'Opening price for the trading day';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_stock_prices"."high_price" IS 'Highest price of the day';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_stock_prices"."low_price" IS 'Lowest price of the day';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_stock_prices"."close_price" IS 'Closing price for the trading day';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_stock_prices"."adj_close_price" IS 'Closing price adjusted for corporate actions';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_stock_prices"."volume" IS 'Total trading volume for the day';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_stock_prices"."dividends" IS 'Dividend amount paid on that date';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_stock_prices"."stock_splits" IS 'Stock split ratio for the day';

COMMENT ON TABLE "model.market_data_pipeline.fact_daily_trading_summary" IS 'Fact table for daily trading summaries';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_trading_summary"."daily_summary_key" IS 'Surrogate key for daily summary';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_trading_summary"."data_date" IS 'Date of the summary';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_trading_summary"."customer_id" IS 'Customer identifier';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_trading_summary"."asset_symbol" IS 'Asset symbol';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_trading_summary"."total_amount" IS 'Total transaction amount for the day';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_trading_summary"."total_transaction_count" IS 'Total number of transactions for the day';

COMMENT ON COLUMN "model.market_data_pipeline.fact_daily_trading_summary"."activity_level" IS 'Activity level for the day';

COMMENT ON TABLE "model.market_data_pipeline.fact_news" IS 'Fact table for news articles';

COMMENT ON COLUMN "model.market_data_pipeline.fact_news"."date" IS 'Publication date and time of the article';

COMMENT ON COLUMN "model.market_data_pipeline.fact_news"."ticker" IS 'Stock ticker symbol associated with the news';

COMMENT ON COLUMN "model.market_data_pipeline.fact_news"."asset_type" IS 'Type of financial asset';

COMMENT ON COLUMN "model.market_data_pipeline.fact_news"."url" IS 'Original URL of the news article';

COMMENT ON COLUMN "model.market_data_pipeline.fact_news"."title" IS 'Headline or title of the news article';

COMMENT ON COLUMN "model.market_data_pipeline.fact_news"."description" IS 'Summary or body content of the news article';

COMMENT ON COLUMN "model.market_data_pipeline.fact_news"."image" IS 'Image URL associated with the article';

COMMENT ON COLUMN "model.market_data_pipeline.fact_news"."category" IS 'Category of the news article';

COMMENT ON TABLE "model.market_data_pipeline.fact_transactions" IS 'Fact table for all transactions';

COMMENT ON COLUMN "model.market_data_pipeline.fact_transactions"."transaction_id" IS 'Business key for transaction';

COMMENT ON COLUMN "model.market_data_pipeline.fact_transactions"."transaction_type" IS 'Type of transaction (BUY or SELL)';

COMMENT ON COLUMN "model.market_data_pipeline.fact_transactions"."transaction_amount" IS 'Transaction amount';

COMMENT ON COLUMN "model.market_data_pipeline.fact_transactions"."transaction_timestamp" IS 'Timestamp of the transaction';

COMMENT ON COLUMN "model.market_data_pipeline.fact_transactions"."data_date" IS 'Date of the transaction';

COMMENT ON COLUMN "model.market_data_pipeline.fact_transactions"."transaction_key" IS 'Surrogate key for transaction';

COMMENT ON COLUMN "model.market_data_pipeline.fact_transactions"."customer_key" IS 'Foreign key to customer dimension';

COMMENT ON COLUMN "model.market_data_pipeline.fact_transactions"."asset_key" IS 'Foreign key to asset dimension';

COMMENT ON TABLE "model.market_data_pipeline.stg_customers" IS 'Staging model for raw customers data';

COMMENT ON COLUMN "model.market_data_pipeline.stg_customers"."customer_id" IS 'Unique customer identifier';

COMMENT ON COLUMN "model.market_data_pipeline.stg_customers"."email" IS 'Customer email address';

COMMENT ON COLUMN "model.market_data_pipeline.stg_customers"."customer_tier" IS 'Customer tier';

COMMENT ON COLUMN "model.market_data_pipeline.stg_customers"."risk_tolerance" IS 'Customer risk tolerance';

COMMENT ON COLUMN "model.market_data_pipeline.stg_customers"."full_name" IS 'Customer full name';

COMMENT ON TABLE "model.market_data_pipeline.stg_transactions" IS 'Staging model for raw transactions data';

COMMENT ON COLUMN "model.market_data_pipeline.stg_transactions"."asset_symbol" IS 'Asset symbol (e.g., AAPL, BTC)';

COMMENT ON COLUMN "model.market_data_pipeline.stg_transactions"."asset_type" IS 'Type of asset (STOCK or CRYPTO)';

COMMENT ON COLUMN "model.market_data_pipeline.stg_transactions"."customer_id" IS 'Customer identifier';

COMMENT ON COLUMN "model.market_data_pipeline.stg_transactions"."net_quantity" IS 'Net quantity (positive for buy, negative for sell)';

COMMENT ON COLUMN "model.market_data_pipeline.stg_transactions"."net_transaction_amount" IS 'Net transaction amount (positive for buy, negative for sell)';

COMMENT ON COLUMN "model.market_data_pipeline.stg_transactions"."transaction_id" IS 'Unique transaction identifier';

COMMENT ON COLUMN "model.market_data_pipeline.stg_transactions"."transaction_type" IS 'Type of transaction (BUY or SELL)';

ALTER TABLE "model.market_data_pipeline.fact_daily_crypto_prices" ADD FOREIGN KEY ("symbol") REFERENCES "model.market_data_pipeline.stg_crypto" ("unknown");

ALTER TABLE "model.market_data_pipeline.dim_crypto" ADD FOREIGN KEY ("symbol") REFERENCES "model.market_data_pipeline.stg_crypto" ("unknown");

ALTER TABLE "model.market_data_pipeline.fact_daily_stock_prices" ADD FOREIGN KEY ("ticker") REFERENCES "model.market_data_pipeline.stg_stocks" ("unknown");

ALTER TABLE "model.market_data_pipeline.dim_stocks" ADD FOREIGN KEY ("ticker") REFERENCES "model.market_data_pipeline.stg_stocks" ("unknown");

ALTER TABLE "model.market_data_pipeline.fact_news" ADD FOREIGN KEY ("ticker") REFERENCES "model.market_data_pipeline.stg_news" ("unknown");

ALTER TABLE "model.market_data_pipeline.fact_news" ADD FOREIGN KEY ("url") REFERENCES "model.market_data_pipeline.fact_customer_portfolio" ("latest_news_url");

ALTER TABLE "model.market_data_pipeline.fact_daily_stock_prices" ADD FOREIGN KEY ("close_price") REFERENCES "model.market_data_pipeline.fact_customer_portfolio" ("current_market_value");

ALTER TABLE "model.market_data_pipeline.fact_customer_portfolio" ADD FOREIGN KEY ("current_market_value") REFERENCES "model.market_data_pipeline.fact_daily_crypto_prices" ("close_price");

ALTER TABLE "model.market_data_pipeline.fact_customer_portfolio" ADD FOREIGN KEY ("asset_symbol") REFERENCES "model.market_data_pipeline.dim_assets" ("asset_symbol");

ALTER TABLE "model.market_data_pipeline.fact_customer_portfolio" ADD FOREIGN KEY ("first_transaction_date") REFERENCES "model.market_data_pipeline.fact_transactions" ("transaction_timestamp");

ALTER TABLE "model.market_data_pipeline.stg_customers" ADD FOREIGN KEY ("customer_id") REFERENCES "model.market_data_pipeline.dim_customers_scd2" ("customer_id");

ALTER TABLE "model.market_data_pipeline.fact_customer_portfolio" ADD FOREIGN KEY ("customer_id") REFERENCES "model.market_data_pipeline.dim_customers_scd2" ("customer_id");

ALTER TABLE "model.market_data_pipeline.fact_daily_trading_summary" ADD FOREIGN KEY ("customer_id") REFERENCES "model.market_data_pipeline.stg_transactions" ("customer_id");

ALTER TABLE "model.market_data_pipeline.fact_daily_trading_summary" ADD FOREIGN KEY ("asset_symbol") REFERENCES "model.market_data_pipeline.stg_transactions" ("asset_symbol");

ALTER TABLE "model.market_data_pipeline.dim_assets" ADD FOREIGN KEY ("asset_name") REFERENCES "model.market_data_pipeline.fact_daily_trading_summary" ("asset_name");

ALTER TABLE "model.market_data_pipeline.dim_customers_scd2" ADD FOREIGN KEY ("customer_tier") REFERENCES "model.market_data_pipeline.fact_daily_trading_summary" ("customer_tier");

ALTER TABLE "model.market_data_pipeline.fact_transactions" ADD FOREIGN KEY ("transaction_id") REFERENCES "model.market_data_pipeline.dim_customers_scd2" ("customer_id");

ALTER TABLE "model.market_data_pipeline.fact_transactions" ADD FOREIGN KEY ("asset_symbol") REFERENCES "model.market_data_pipeline.dim_assets" ("asset_symbol");

ALTER TABLE "model.market_data_pipeline.fact_transactions" ADD FOREIGN KEY ("transaction_id") REFERENCES "model.market_data_pipeline.stg_transactions" ("transaction_id");
