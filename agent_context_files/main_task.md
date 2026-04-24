

1. Choose a small mix of cities in the US:
    - LOS ANGELES, CA
    - CHICAGO, IL
    - COLUMBUS, OH
    - JACKSONVILLE, FL
    - PORTLAND, OR

2. Get weather forcast data for these cities for the coming 10 days.
    - Extract only data for chosen cities from step 1
        - date
        - postal code / zip code
        - city
        - state
        - temperature
            - max
            - min
            - avg
        - cloud cover
        - percipitation total
        - rain
        - snow
        - UV
        - humidity
        - wind
        - others if available

    - Create a table with weather data per field per day per city&state 
        - the data contains many data points from different postcodes in the same city, but it should be one row per city&state and date

3. Document data fields in weather forecast data
    - schema
    - data types

4. Get Historical weather data
    - use hackthon_group_a2_accuweather_historical_weather_data_u_s_postal_codes_sample
    - Match chosen data with the forecast data in terms of weather indicators
    - Extract only data for chosen cities from step 1
    - Create a table with weather data per field per day per city&state 
        - the data contains many data points from different postcodes in the same city, but it should be one row per city&state and date

5.  Get Historical sales data
    - use hackthon_group_a2_databricks_simulated_retail_customer_data
        - start with customer data (v01.customers)
            - if any cities are null
                - try to search for the city based on the postcode
                    - if any postcode end with ".0" only use the data before
            - extract only customers in the cities from step 1
        - left join with sales (v01.sales_orders) with customer_id
        - extract a sum of total sales amount from price in ordered_products
        - convert order_datetime from int to datetime in YEAR-MONTH-DAY-format 
        - extract a full month (2019-10-31)
        - change year and month of the order to year 2022 and month 01
            - eg: "2019-10-14" should be converted to "2022-01-14"
        - Create an aggreageted table of sales amount per day per city&state.


6. Join the two datasets on date and city&state

7. Create an ML model best suited for predicting sales amount based on historical sales and weather.

8. Apply the model to the forecast weather data and save the forcasted sales amounts per day per city.

9. Create an a simple interactive app (accessable through a browser)
    - The user should be able to choose one of the 5 cities and get an estimate for each of the coming 10 days and what the weather is forcasted to be for each day.
