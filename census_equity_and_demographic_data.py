#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""
This script downloads census data exports county level data to an excel file and tract level data to shapefiles.
Other notes: 
This script will be built out to include all relevant census variables. 
Environment is based off of Pin-Up environment. Jupytext and o


# In[293]:


#import necessary libraries

import requests
import pandas as pd
import geopandas as gpd
import numpy as np
import geojson
import folium
import os


# In[493]:


#Set output filepaths
Transportation_Tracts = r'C:\Egnyte\Shared\PROJECTS\2020\00-2020-088 Sacramento County CA ATP\GIS\Data\From_Alta\Census_data\equity_tracts\transportation_metrics\transportation_tracts.shp'

Age_Tracts = r"C:\Egnyte\Shared\PROJECTS\2020\00-2020-088 Sacramento County CA ATP\GIS\Data\From_Alta\Census_data\equity_tracts\age\tract_age.shp"

EXCEL_FP = r'C:\Egnyte\Shared\PROJECTS\2020\00-2020-088 Sacramento County CA ATP\GIS\Data\From_Alta\Census_data\countylevel_demographics.xlsx'


# In[364]:


#These are all of the main variables used to build a call url to the the census api website
#Available APIs (--> 2018 ACS Detailed Tables Variables [ html | xml | json ])
#https://www.census.gov/data/developers/data-sets.html

HOST = "https://api.census.gov/data"
year = "2018"
#dataset = "acs/acs5/subject"
dataset = "acs/acs5"
api_key = "f9e79198302081250c07d556f35d8a81cdae528a"
base_url = "/".join([HOST, year, dataset,])


# In[365]:


#These 'predicates' help build the more complex query to the query. 
#Notice that the variables (columns) are specified here, the column names for our new dataframe
#are also created here. The predicate dictionary keys are then assigned values, which will be passed
#into the request.get function. 


# In[391]:


# Setup request for Table B08006: Sex of Workers by Means of Transportation to Work
# Setup request for Table B08014: SEX OF WORKERS BY VEHICLES AVAILABLE (Total and No vehicle columns only)
# (COUNTY LEVEL)
request_predicates = {}
get_vars_transpo_mode = ["NAME","B08006_001E","B08006_002E","B08006_003E", "B08006_004E",
                         "B08006_008E","B08006_014E","B08006_015E","B08006_016E", 
                         "B08006_017E", "GEO_ID", "B08014_001E", "B08014_002E" 
                        ]
col_names_transpo_mode = ["place_name", "total","total_car_truck_van",
                          "car_truck_van_drove_alone","car_truck_van_carpooled",
                          "public_transportation", "bike", "walk", "taxi_moto_other",
                          "work_from_home","geoid", "total_workers", "no_vehicle",
                          "state_code", "county"
                         ]

request_predicates["key"] = api_key
request_predicates["get"] = ",".join(get_vars_transpo_mode)


#THESE PREDICATES GET AGGREGATE TOTALS FOR ENTIRE COUNTY (SACRAMENTO)
request_predicates["for"] = "county:067"
request_predicates["in"] = "state:06"

transpo_mode_county = requests.get(base_url, params=request_predicates)
num_columns = 15


# In[392]:


#Setting up Means of Transportation to Workdata frame, getting rid of first header row
df_transpo_mode = pd.DataFrame(columns=col_names_transpo_mode, data=transpo_mode.json()[1:])


# In[393]:


df_transpo_mode.head(5)


# In[368]:


# THESE PREDICATES GET DATA FOR EVERY TRACT in Sacramento County
# These revised predicates replace the 'for' and 'in' predicates above.
#(TRACT LEVEL)

request_predicates["for"] = "tract:*"
request_predicates["in"] = "state:06+county:067"
transpo_mode_tracts = requests.get(base_url, params=request_predicates)



# In[369]:


#Setting up Means of Transportation to Work at Tract Level data frame, getting rid of first header row
#This includes appending 'tract' to columns list
col_names_transpo_mode = ["place_name", "total","total_car_truck_van",
                          "car_truck_van_drove_alone","car_truck_van_carpooled",
                          "public_transportation", "bike", "walk", "taxi_moto_other",
                          "work_from_home","geoid", "total_workers", "no_vehicle","state_code",
                          "county", "tract" 
                         ]
df_transpo_mode_tracts = pd.DataFrame(columns=col_names_transpo_mode, data=transpo_mode_tracts.json()[1:])
num_columns = 16


# In[324]:


# Join the census data to Tigerline Cartographic Boundary census tract geometries. 
# Census Tract Tigerline California Census Tracts location (ftp url included in notes below)

census_tracts_shp = r"C:\Egnyte\Shared\SERVICE_AREA\Data Science\data\Census_Data\Carto_boundaries_2018_California\cb_2018_06_tract_500k.shp"

# Verify that filepath is correct
os.path.isfile(census_tracts_shp)

#This reads the census tracts shapefile into a geodataframe
gdf = gpd.read_file(census_tracts_shp)

#Make tigerline boundary columns lowercase
gdf.columns = map(str.lower, gdf.columns)


# In[325]:


#The geoid field in the df_transpo_mode table does not match the Tigerlines geoid field. 
#This slices the the right 11 most digits, which match the geoid codes in the TigerLine file. 
#(... these are state ('06') for California, followed by county, followed by census tract)

df_transpo_mode_tracts.insert(num_columns, "geoid_join",df_transpo_mode_tracts['geoid'].str.slice(-11), True) 



# In[326]:


#Join table with the tigerline data 
#Note:gdf must be left table (the table that merge method is run on) 
#so that a geodataframe (not a dataframe) is returned. 

df_transpo_mode_with_geom = gdf.merge(df_transpo_mode_tracts,left_on='geoid',right_on='geoid_join')


# In[327]:


df_transpo_mode_with_geom.to_file(Transportation_Tracts)


# In[328]:


print(df_transpo_mode_tracts.shape)
print(df_transpo_mode.shape)


# In[476]:


## SETUP FOR TABLE B08001: SEX BY AGE
#NAME dropped from request because only 50 variables per request are permitted.
request_predicates = {}
get_vars_age = ["B01001_001E", "B01001_002E","B01001_003E", "B01001_004E","B01001_005E", 
                "B01001_006E", "B01001_007E","B01001_008E", "B01001_009E","B01001_010E", 
                "B01001_011E", "B01001_012E","B01001_013E", "B01001_014E","B01001_015E", 
                "B01001_016E", "B01001_017E","B01001_018E", "B01001_019E","B01001_020E", 
                "B01001_021E", "B01001_022E","B01001_023E", "B01001_024E","B01001_025E", 
                "B01001_026E", "B01001_027E","B01001_028E", "B01001_029E","B01001_030E", 
                "B01001_031E", "B01001_032E","B01001_033E", "B01001_034E","B01001_035E", 
                "B01001_036E", "B01001_037E","B01001_038E", "B01001_039E","B01001_040E", 
                "B01001_041E", "B01001_042E","B01001_043E", "B01001_044E","B01001_045E", 
                "B01001_046E", "B01001_047E","B01001_048E", "B01001_049E","GEO_ID"
                ]

col_names_age = ["total_pop","total_male","tl_m0_5","tl_m5_9", "tl_m10_14", "tl_m15_17", 
                 "tl_m18_19", "tl_m20", "tl_m21", "tl_m22_24", "tl_m25_29", "tl_m30_34",
                 "tl_m35_39", "tl_m40_44", "tl_m45_49", "tl_m50_54", "tl_m55_59", "tl_m60_61",
                 "tl_m62_64", "tl_m65_66", "tl_m67_69", "tl_m70_74", "tl_m75_79", "tl_m80_84",
                 "tl_m85_pl","total_female", "tl_f0_5","tl_f5_9", "tl_f10_14", "tl_f15_17", 
                 "tl_f18_19", "tl_f20", "tl_f21", "tl_f22_24", "tl_f25_29", "tl_f30_34",
                 "tl_f35_39", "tl_f40_44", "tl_f45_49", "tl_f50_54", "tl_f55_59", "tl_f60_61",
                 "tl_f62_64", "tl_f65_66", "tl_f67_69", "tl_f70_74", "tl_f75_79", "tl_f80_84",
                 "tl_f85_pl","geoid","state","county"
                ]

request_predicates["key"] = api_key
request_predicates["get"] = ",".join(get_vars_age)

#THESE PREDICATES GET AGGREGATE TOTALS FOR ENTIRE COUNTY (SACRAMENTO)
request_predicates["for"] = "county:067"
request_predicates["in"] = "state:06"


age_county = requests.get(base_url, params=request_predicates)
num_columns = 49


# In[477]:


#Setting up AGE data frame, getting rid of first header row
df_age_county = pd.DataFrame(columns=col_names_age, data=age_county.json()[1:])


# In[478]:


df_age_county.head(5)


# In[479]:


# THESE PREDICATES GET DATA FOR EVERY TRACT

request_predicates["for"] = "tract:*"
request_predicates["in"] = "state:06+county:067"
age_tracts = requests.get(base_url, params=request_predicates)
num_columns = 50


# In[480]:


#Setting up AGE TRACTS data frame, getting rid of first header row
col_names_age = ["total_pop","total_male","tl_m0_5","tl_m5_9", "tl_m10_14", "tl_m15_17", 
                 "tl_m18_19", "tl_m20", "tl_m21", "tl_m22_24", "tl_m25_29", "tl_m30_34",
                 "tl_m35_39", "tl_m40_44", "tl_m45_49", "tl_m50_54", "tl_m55_59", "tl_m60_61",
                 "tl_m62_64", "tl_m65_66", "tl_m67_69", "tl_m70_74", "tl_m75_79", "tl_m80_84",
                 "tl_m85_pl","total_female", "tl_f0_5","tl_f5_9", "tl_f10_14", "tl_f15_17", 
                 "tl_f18_19", "tl_f20", "tl_f21", "tl_f22_24", "tl_f25_29", "tl_f30_34",
                 "tl_f35_39", "tl_f40_44", "tl_f45_49", "tl_f50_54", "tl_f55_59", "tl_f60_61",
                 "tl_f62_64", "tl_f65_66", "tl_f67_69", "tl_f70_74", "tl_f75_79", "tl_f80_84",
                 "tl_f85_pl","geoid","state","county", 'tract'
                ]
df_age_tracts = pd.DataFrame(columns=col_names_age, data=age_tracts.json()[1:])


# In[481]:


print(age_tracts)


# In[482]:


df_age_tracts.head(5)


# In[483]:


# using dictionary to convert specific columns 
# convert_dict = {'A': int, 
#                 'C': float
#                } 
  
# df = df.astype(convert_dict) 
# print(df.dtypes) 
dtype_conversion = { "total_pop": int,
                    "total_male": int,
                    "tl_m0_5": int,
                    "tl_m5_9": int,
                    "tl_m10_14": int,
                    "tl_m15_17": int,
                    "tl_m18_19": int, 
                    "tl_m20": int, 
                    "tl_m21": int, 
                    "tl_m22_24": int, 
                    "tl_m25_29": int, 
                    "tl_m30_34": int,
                    "tl_m35_39": int, 
                    "tl_m40_44": int, 
                    "tl_m45_49": int, 
                    "tl_m50_54": int, 
                    "tl_m55_59": int, 
                    "tl_m60_61": int,
                    "tl_m62_64": int, 
                    "tl_m65_66": int, 
                    "tl_m67_69": int, 
                    "tl_m70_74": int, 
                    "tl_m75_79": int, 
                    "tl_m80_84": int,
                    "tl_m85_pl": int,
                    "total_female": int, 
                    "tl_f0_5": int,
                    "tl_f5_9": int,
                    "tl_f10_14": int,
                    "tl_f15_17": int,
                    "tl_f18_19": int,
                    "tl_f20": int,
                    "tl_f21": int,
                    "tl_f22_24": int,
                    "tl_f25_29": int,
                    "tl_f30_34": int,
                    "tl_f35_39": int,
                    "tl_f40_44": int,
                    "tl_f45_49": int,
                    "tl_f50_54": int,
                    "tl_f55_59": int,
                    "tl_f60_61": int,
                    "tl_f62_64": int,
                    "tl_f65_66": int,
                    "tl_f67_69": int,
                    "tl_f70_74": int,
                    "tl_f75_79": int,
                    "tl_f80_84": int,
                    "tl_f85_pl": int,
                    "county": int,
                    'tract': int
                }
df_age_tracts = df_age_tracts.astype(dtype_conversion) 


# In[484]:


age_5_under = df_age_tracts["tl_m0_5"] + df_age_tracts["tl_f0_5"]

age_65_over = df_age_tracts["tl_f65_66"] + df_age_tracts["tl_f67_69"] 
+ df_age_tracts["tl_f70_74"] + df_age_tracts["tl_f75_79"] 
+ df_age_tracts["tl_f80_84"] + df_age_tracts["tl_f85_pl"]
df_age_tracts["tl_m65_66"] + df_age_tracts["tl_m67_69"] 
+ df_age_tracts["tl_m70_74"] + df_age_tracts["tl_m75_79"] 
+ df_age_tracts["tl_m80_84"] + df_age_tracts["tl_m85_pl"]

age_vulnerable = age_5_under + age_65_over



# In[485]:


df_age_tracts["age_5_under"] = age_5_under 
df_age_tracts["age_65_over"] = age_65_over
df_age_tracts["age_vulnerable"] = age_vulnerable



# In[486]:


df_age_tracts_simple = df_age_tracts[["total_pop","age_5_under","age_65_over","age_vulnerable","geoid","state","county","tract"]]


# In[487]:


df_age_tracts_simple.head(5)


# In[488]:


#The geoid field in the df_transpo_mode table does not match the Tigerlines geoid field. 
#This slices the the right 11 most digits, which match the geoid codes in the TigerLine file. 
#(... these are state ('06') for California, followed by county, followed by census tract)

#This reads the census tracts shapefile into a geodataframe
gdf = gpd.read_file(census_tracts_shp)

#Make tigerline boundary columns lowercase
gdf.columns = map(str.lower, gdf.columns)

df_age_tracts_simple.insert(8, "geoid_join",df_transpo_mode_tracts['geoid'].str.slice(-11), True) 


# In[489]:


df_age_tracts_simple


# In[490]:


#Join table with the tigerline data 
#Note:gdf must be left table (the table that merge method is run on) 
#so that a geodataframe (not a dataframe) is returned. 

df_age_tracts_simple_with_geom = gdf.merge(df_age_tracts_simple,left_on='geoid',right_on='geoid_join')


# In[491]:


df_age_tracts_simple_with_geom.to_file(Age_Tracts)


# In[494]:


#Export county level dataframes to excel

with pd.ExcelWriter(EXCEL_FP) as writer:
    df_age_county.to_excel(writer, sheet_name='county_age')
    df_transpo_mode.to_excel(writer, sheet_name='county_transpo_mode')

# OTHER EXAMPLES FOUND WHILE TROUBLESHOOTING
# with pd.ExcelWriter("test.xlsx", engine='openpyxl', mode='a') as writer:
#     df.to_excel(writer)
# with pd.ExcelWriter(Excel_FP) as writer:
#     bike_crashes_by_year.to_excel(writer, sheet_name='b_crashes_by_year')


# In[218]:


print(transpo_mode_tracts)


# In[210]:


print(age)


# In[211]:


#Setting up AGE data frame, getting rid of first header row
df_age = pd.DataFrame(columns=col_names_age, data=age.json()[1:])


# In[212]:


df_age.head(5)


# In[164]:


#Setting up Means of Transportation to Workdata frame, getting rid of first header row
df_transpo_mode = pd.DataFrame(columns=col_names_transpo_mode, data=transpo_mode.json()[1:])


# In[219]:





# In[220]:


df_transpo_mode_tracts.shape


# In[165]:


df_transpo_mode.shape


# In[166]:





# In[171]:


df_transpo_mode


# In[156]:





# In[157]:


#split the place_name to get human known county names
#str.split splits on comma  (',') delimiter. .str[1] selects the second element in the list (the county name) 
df_transpo_mode.insert(1, "county_name",df_transpo_mode['place_name'].str.split(',').str[1].str.strip(), True)
num_columns += 1


# In[158]:


df_transpo_mode


# In[59]:



study_counties = ['Sacramento County']
tracts_select_counties = df_transpo_mode.loc[df_transpo_mode['county_name'].isin(study_counties)]


# In[61]:


tracts_select_counties.shape


# In[62]:


print(df_transpo_mode)


# In[25]:


df_transpo_mode_with_geom['bike'] = df_transpo_mode_with_geom['bike'].astype(int)


# In[26]:


df_transpo_mode_with_geom['total'] = df_transpo_mode_with_geom['total'].astype(int)


# In[57]:


# Create a Geo-id which is needed by the Folium (it needs to have a unique identifier for each row)
# We do not want the GeoJson object created earlier. Use original df_transpo_mode_with_geom data.
#census_tracts_gjson = folium.features.GeoJson(df_transpo_mode_with_geom, name="census tracts")
df_transpo_mode_with_geom['geoid'] = df_transpo_mode_with_geom.index.astype(str)



# In[31]:


#calculate percentage of people that bike to work in each tract
a = (df_transpo_mode_with_geom['bike'] / df_transpo_mode_with_geom['total'])*100


# In[32]:


df_transpo_mode_with_geom.insert(2,'pct_bike',a, True)


# In[33]:


# Select only needed columns
choropleth_data = df_transpo_mode_with_geom[['geoid', 'bike', 'pct_bike', 'geometry']]

# Convert to geojson (not needed for the simple coropleth map!)
#pop_json = data.to_json()

#check data
choropleth_data.head()


# In[34]:


choropleth_data['geoid'] = choropleth_data.index.astype(str)


# In[37]:


bounds = df_transpo_mode_with_geom.total_bounds
a = np.mean(bounds[0:3:2]).round(3)
b = np.mean(bounds[1:4:2]).round(3)
data_centroid = [b,a]
print(data_centroid)


# In[39]:


# Create a Map instance
m = folium.Map(location=data_centroid, tiles = 'cartodbpositron', zoom_start=10, control_scale=True)

#Plot a choropleth map
#Notice: 'geoid' column that we created earlier needs to be assigned always as the first column
folium.Choropleth(
    geo_data=choropleth_data,
    name='Percentage of Cyclists',
    data=choropleth_data,
    columns=['geoid', 'pct_bike'],
    key_on='feature.id',
    fill_color='YlOrRd',
    fill_opacity=0.7,
    line_opacity=0.2,
    line_color='white',
    line_weight=0,
    highlight=False,
    smooth_factor=1.0,
    #threshold_scale=[1, 2, 3, 4, 5],
    legend_name= 'Percentage of workers that bike to work').add_to(m)


# In[42]:


# Convert points to GeoJson
# This creates interactive labels
folium.features.GeoJson(choropleth_data,
                        name='Labels',
                        style_function=lambda x: {'color':'transparent','fillColor':'transparent','weight':0},
                        tooltip=folium.features.GeoJsonTooltip(fields=['pct_bike'],
                                                               # aliases = ['Population'],
                                                                labels=True,
                                                                sticky=False
                                                                            )
                       ).add_to(m)


# In[40]:


#Show map
m


# In[ ]:


#SOURCES
#https://www.w3schools.com/tags/ref_urlencode.ASP
#https://www.census.gov/content/dam/Census/data/developers/api-user-guide/api-guide.pdf
#https://api.census.gov/data/2018/acs/acs5/variables.html
#https://api.census.gov/data/2018/acs/acs5/subject/variables.html
#https://www.youtube.com/watch?v=Wi0_Mb0e4JM
#https://atcoordinates.info/2019/09/24/examples-of-using-the-census-bureaus-api-with-python/
#--downloading tigerline from zip -- 
#http://andrewgaidus.com/Dot_Density_County_Maps/
#--Python for reading zip tigerline shpfile --
#http://andrewgaidus.com/Reading_Zipped_Shapefiles/
#--Aaron's ATP Data Mining Project would also be useful--
#https://github.com/AltaPlanning/GIS-notebooks/tree/master/2020-000%20ATP%20Data%20Mining
#https://automating-gis-processes.github.io/site/notebooks/L5/interactive-map-folium.html

#-- geographies and summary levels --
#https://censusreporter.org/topics/geography/
#geo_ids=140|04000US06  --> this should be a all tracts in California

#There is no great way to use the api to return census tract geometries: the geography api functions 
#only seem to allow calling a specific geoid. One option would be to loop through geoids and call census reporter
#to request geography for each geoid, but that would involve a lot of calls. 
#https://api.censusreporter.org/1.0/data/show/latest?table_ids=B01001&geo_ids=140|04000US06
#error"You requested 8057 geoids. The maximum is 3500. Please contact us for bulk data."



# MEDIAN EARNINGS IN THE PAST 12 MONTHS (IN 2018 INFLATION-ADJUSTED DOLLARS) BY MEANS OF TRANSPORTATION TO WORK
# Survey/Program: American Community Survey
# Universe: Workers 16 years and over with earnings
# Year: 2018
# Estimates: 1-Year
# Table ID: B08121



# In[ ]:


# ### OTHER NOTES
#I was a bit confused about obtaining the Census Tiger boundaries. The Tigerweb REST service seemed geared towards
#delivering Web Map Service (WMS) map images. We want the spatial data! Hopefully the stack exchange post linked below
#clears some of the confusion up.
#https://gis.stackexchange.com/questions/269650/how-to-bring-the-tiger-census-reporter-api-to-geopandas
#--->I'm not sure if these geometries still exist on the census api. 
#zipfiles can be downloaded at the ftp site below. I am using blog and aaron's atp data mining python as examples. 
#For now I am just going to manually unzip census geometry, but a link to a tutorial is included below to automate
#download, unzipping, and processing the geometry. 

#ftp://ftp2.census.gov/geo/tiger/TIGER2018/TRACT/tl_2018_06_tract.zip

