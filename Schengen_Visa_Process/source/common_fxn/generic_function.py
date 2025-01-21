# Databricks notebook source
pip install pycountry-convert fuzzywuzzy


# COMMAND ----------

pip install python-Levenshtein

# COMMAND ----------

import pycountry
from fuzzywuzzy import process

# COMMAND ----------

def get_country_code(country_name):
    try:
        country = pycountry.countries.get(name=country_name)
        if country:
            return country.alpha_3
        else:
            matches = process.extractOne(country_name, [country.name for country in pycountry.countries])
            if matches:
                best_match = matches[0]
                country = pycountry.countries.get(name=best_match)
                return country.alpha_3 if country else None
    except Exception as e:
        return None

# COMMAND ----------

def validate_country(country_name):
    try:
        country = pycountry.countries.lookup(country_name)
        return country.name
    except LookupError:
        return None

# COMMAND ----------

import pycountry_convert as pc

def get_continent(country_name):
    try:
        country_code = pc.country_name_to_country_alpha2(country_name)
        continent_code = pc.country_alpha2_to_continent_code(country_code)
        continent_name = pc.convert_continent_code_to_continent_name(continent_code)
        return continent_name
    except:
        return None
