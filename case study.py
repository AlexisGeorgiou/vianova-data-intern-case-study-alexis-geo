#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Alexis Georgiou
# The code uses python:
#    1) to fetch the data, then store it to a local postgreSQL database
#    2) queries the data from the fetched csv and stores the answer in postgreSQL database
#    3) saves the answer on a csv

# The code can update the database every time it runs, it will update any value changed in the future or any new city.
# The code uses SQL to:
#    1) Create tables
#    2) Update correctly the table after every weekly run
#    3) answer and store the query

#    Note: We could use a .py file to run this every week, I used a notebook for more readability
#    I did not included every column to be stored in the database for demonstration and readability


# In[2]:


import pandas as pd
import psycopg2


# # Fetching the data

# In[3]:


#Use url to fetch, or filename to read locally
filename = 'geonames-all-cities-with-a-population-1000.csv'
url = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/geonames-all-cities-with-a-population-1000/exports/csv?lang=en&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
df = pd.read_csv(url, sep = ';')

print(df.head())


# # Connect to database and store the fetched data

# In[4]:


# Connect to a PostgreSQL database
conn = psycopg2.connect(
    host='localhost',
    port='5432',
    database='postgres',
    user='postgres',
    password='1234'
)
# Create a cursor
cur = conn.cursor()


# In[5]:


cur = conn.cursor()
#Create a table with the csv info (have to declare datatype of each column, for demonstration we make 4 columns)
cur.execute('''
    CREATE TABLE IF NOT EXISTS city_population (
        geoname_id INT NOT NULL PRIMARY KEY,
        city_name TEXT NOT NULL,
        country_name TEXT,
        country_code TEXT,
        population INT
    );
''')

# Replace the data into the table
# This will insert new data if we have a new geoname_id (e.g new city is added)
# This will replace the rows of data that already exist (e.g population for a city is increased), it won't add new records again
for _, row in df.iterrows():
    cur.execute('''
        INSERT INTO city_population (geoname_id, city_name, country_name, country_code, population)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT(geoname_id) 
        DO UPDATE SET 
            city_name = EXCLUDED.city_name,
            country_name = EXCLUDED.country_name,
            country_code = EXCLUDED.country_code,
            population = EXCLUDED.population;
    ''', (row['Geoname ID'], row['Name'], row['Country name EN'], row['Country Code'], row['Population']))
    
# Commit the changes
conn.commit()
cur.close()


# # Answer the query using python pandas

# In[6]:


#Keeping only rows we are interested
df1 = df[['Population', 'Country Code']]

#Identifying countries that have a megapolis
big_cities_df = df1[df1['Population'] > 10_000_000]
bad_countries = []
for index, row in big_cities_df.iterrows():
    population = row['Population']
    country_code = row['Country Code']
    if population >= 10_000_000:
        bad_countries.append(country_code)

bad_countries = list(set(bad_countries))
bad_countries


# In[7]:


# Get the unique countries frame
df2 = df[['Country name EN', 'Country Code']].drop_duplicates()

# Remove rows with bad_countries codes
df2 = df2[~df2['Country Code'].isin(bad_countries)]

# Sort by country name
df2 = df2.sort_values(by=['Country name EN'])

#Save a csv of the answer
df2.to_csv('result.csv', sep='\t', index=False)


# In[8]:


df2


# # Store the answered query in the database

# In[9]:


#Drop previous answer
cur = conn.cursor()
cur.execute('''
    DROP TABLE IF EXISTS no_megapolis_countries;
''')
cur.close()

cur = conn.cursor()
# Create the table of the query answer
cur.execute('''
    CREATE TABLE IF NOT EXISTS no_megapolis_countries (
        country_name TEXT,
        country_code TEXT NOT NULL PRIMARY KEY
    );
''')

# Store the dataframe into the answer table
for _, row in df2.iterrows():
    cur.execute('''
        INSERT INTO no_megapolis_countries (country_name, country_code)
        VALUES (%s, %s)
    ''', (row['Country name EN'], row['Country Code']))
    
    
# Commit the change
cur.close()
conn.commit()


# # Answer the query with SQL language

# In[10]:


#Drop previous answer
cur = conn.cursor()
cur.execute('''
    DROP TABLE IF EXISTS no_megapolis_countries_with_SQL;
''')
cur.close()


cur = conn.cursor()
#Create a table with the answer
cur.execute('''
    CREATE TABLE IF NOT EXISTS no_megapolis_countries_with_SQL (
        country_name TEXT,
        country_code TEXT NOT NULL PRIMARY KEY
    );
''')

#SQL query answer
cur.execute('''
    INSERT INTO no_megapolis_countries_with_SQL (country_name, country_code)
    SELECT country_name, country_code
    FROM city_population
    GROUP BY country_name, country_code
    HAVING MAX(population) <= 10000000
    ORDER BY country_name;
''')

# Commit the changes 
conn.commit()
cur.close()


# # Close connection

# In[11]:



# Close the connection
conn.close()

