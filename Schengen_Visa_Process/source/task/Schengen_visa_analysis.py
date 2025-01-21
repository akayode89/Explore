# Databricks notebook source
pip install pycountry-convert fuzzywuzzy

# COMMAND ----------

pip install python-Levenshtein

# COMMAND ----------

import pycountry
from fuzzywuzzy import process
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import pycountry_convert as pc
import plotly.express as px

# COMMAND ----------

src_df = spark.read.csv("/FileStore/src_data/schengen_data.csv", header=True, inferSchema=True)

# COMMAND ----------

display(src_df)

# COMMAND ----------

# Clean up the column name, replace all space,parentesis, and comma with _
new_columns = [c.replace(' ', '_').replace('(', '_').replace(')', '_').replace(',', '_') for c in src_df.columns]

# Update the column name
df = src_df.toDF(*new_columns)

# Select the required column
df = df.selectExpr('Schengen_State as Schengen_Country', 'Country_where_consulate_is_located as Applicant_Country', 'Consulate as Consolate_City', 'Total_ATVs_and_uniform_visas_applied_for as Number_of_Visa_Application', 'Total_ATVs_and_uniform_visas_issued___including_multiple_ATVs__MEVs_and_LTVs_ as Total_Visas_Issued', 'Total_ATVs_and_uniform_visas_not_issued as Visas_Not_Issued', 'Year')

# Drop null or blank record in the dataframe
df = df.dropna(how='all')

# COMMAND ----------

# Create a function that return the country name from the country code 
def validate_country(country_name):
    try:
        country = pycountry.countries.lookup(country_name)
        return country.name
    except LookupError:
        return None

validate_country_udf = udf(validate_country, StringType())

# COMMAND ----------

# Create a function that return the country code from the country name
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

correct_country_name_udf = udf(get_country_code, StringType())

# COMMAND ----------

# Validate the column name to return the correct country name code
df = df.withColumn('Consolute_Country_code', correct_country_name_udf(df.Applicant_Country)) \
        .withColumn('Applicant_Country', validate_country_udf(df.Applicant_Country))

# COMMAND ----------

# Create a function that return the continent name from the country name
def get_continent(country_name):
    try:
        country_code = pc.country_name_to_country_alpha2(country_name)
        continent_code = pc.country_alpha2_to_continent_code(country_code)
        continent_name = pc.convert_continent_code_to_continent_name(continent_code)
        return continent_name
    except:
        return None

get_continent_udf = udf(get_continent, StringType())

# COMMAND ----------

df = df.withColumn('Continent', get_continent_udf(df.Applicant_Country)) \
        .where("Applicant_Country is not null")

# COMMAND ----------

display(df)

# COMMAND ----------

df.createGlobalTempView("schengen_visa")

# COMMAND ----------

## Visialization
df_visa_application_by_continent = spark.sql("""
                         select Year,Continent, sum(Number_of_Visa_Application) as Total_Application from global_temp.schengen_visa where Continent is not null group by Year,Continent order by 3""")
df_visa_application_by_continent = df_visa_application_by_continent.toPandas()

# COMMAND ----------

visa_application_by_continent_by_year =px.bar(df_visa_application_by_continent, x = 'Year', y= 'Total_Application', color = 'Continent')

# COMMAND ----------

visa_application_by_continent_by_year.update_layout(title_text="Total Schengen Visa Application by Continent",
                  xaxis_title="Year",
                  yaxis_title="Total Visa Application",
                  legend_title="Continent")

visa_application_by_continent_by_year.write_html("Schengen_Visa_Application_by_Continent.html")

# COMMAND ----------

## Top 10 country with highest number of Schengen Visa approved in 2019

df_top_10_va_country = spark.sql("""
                              select Applicant_Country, sum(Total_Visas_Issued) as Total_Approved_Visa from global_temp.schengen_visa where Applicant_Country is not null and Year = 2019 group by Applicant_Country order by Total_Approved_Visa desc limit 10
                              """)
df_top_10_va_country = df_top_10_va_country.toPandas()

# COMMAND ----------

bar_chart_top_10_country_approved = px.bar(df_top_10_va_country, x = 'Applicant_Country', y= 'Total_Approved_Visa')

bar_chart_top_10_country_approved.update_layout(title_text="Top 10 Country with highest number of Schengen Visa approved in 2019",
                   xaxis_title="Country",
                   yaxis_title="Total Visa Approved",
                   legend_title="Country")

# COMMAND ----------

df_top_10_vd_country = spark.sql("""
                              select Applicant_Country,sum(Visas_Not_Issued) as Total_denied_Visa from global_temp.schengen_visa where Applicant_Country is not null and Year = 2019 group by Applicant_Country order by Total_denied_Visa desc limit 10
                              """)

# COMMAND ----------

df_top_10_vd_country = df_top_10_vd_country.toPandas()

# COMMAND ----------

bar_chart_top_10_country_denied = px.bar(df_top_10_vd_country, x = 'Applicant_Country', y= 'Total_denied_Visa')

bar_chart_top_10_country_denied.update_layout(title_text="Top 10 Country with highest number of Schengen Visa denied in 2019",
                   xaxis_title="Country",
                   yaxis_title="Total Visa Denied",
                   legend_title="Country")

# COMMAND ----------

df_visa_approval_by_map = spark.sql("""
                                    Select Year,Applicant_Country, sum(Number_of_Visa_Application) as Total_Application,sum(Visas_Not_Issued) as Total_denied_Visa,sum(Total_Visas_Issued) as Total_Approved_Visa from global_temp.schengen_visa where Applicant_Country is not null group by Year,Applicant_Country 
                                    """)

# COMMAND ----------

df_visa_approval_by_map = df_visa_approval_by_map.toPandas()

# COMMAND ----------

map_total_visa_application = px.choropleth(df_visa_approval_by_map, locations="Applicant_Country", 
                     color="Total_Application", 
                     hover_name="Applicant_Country", 
                     animation_frame="Year",
                     range_color=[100000, 100000], 
                     color_continuous_scale=px.colors.sequential.Viridis,
                     locationmode="country names",
                     title="Schengen Visa Application by Country")

display(map_total_visa_application)

# COMMAND ----------

map_denied_visa = px.choropleth(df_visa_approval_by_map, locations="Applicant_Country", 
                     color="Total_denied_Visa", 
                     hover_name="Applicant_Country", 
                     animation_frame="Year",
                     range_color=[100000, 100000], 
                     color_continuous_scale=px.colors.sequential.Viridis,
                     locationmode="country names",
                     title="Schengen Visa Application by Country")

display(map_denied_visa)

# COMMAND ----------

map_approved_visa = px.choropleth(df_visa_approval_by_map, locations="Applicant_Country", 
                     color="Total_Approved_Visa", 
                     hover_name="Applicant_Country", 
                     animation_frame="Year",
                     range_color=[100000, 100000], 
                     color_continuous_scale=px.colors.sequential.Viridis,
                     locationmode="country names",
                     title="Schengen Visa Application by Country")

display(map_approved_visa)
