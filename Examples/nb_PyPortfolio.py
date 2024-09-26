# Databricks notebook source
Get a portfolio analysis running

# COMMAND ----------

import requests

# COMMAND ----------

def get_quote_info(quote):
    yahoo_url = "http://finance.yahoo.com/quote/"
    quote_url = yahoo_url + quote

    page = requests.get(quote_url)

    print(page.text)

# COMMAND ----------

# single testcase
get_quote_info("ASML")

# COMMAND ----------

# portfolio
stocks = [
    ('ASML','ASML')
    ,('TSMC','TSMC')
    ,
] 


