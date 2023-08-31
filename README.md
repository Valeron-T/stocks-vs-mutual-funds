# Stocks vs Mutual Funds
- A script which fetches data of selected stocks and mutual funds from various sources and pushes it to a database of our choice.
- Our objective was to demo an application of ETL (Extract, Transform and Load) pipeline.
- Initially data is obtained is in JSON format. We apply necessary functions to extract only the data we need and transform it as required following which it is pushed into a PostgreSQL database.
- Using github actions, the script executes every 24 hours to as our data source also updates every 24 hrs.
- With the data structured in the format of our choice, we can then perform analytics or other useful operations.
