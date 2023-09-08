#!/usr/bin/env python
# coding: utf-8

# In[1]:


import apache_beam as beam
import psycopg2
import requests
from thelake.forms import BeamForm  
from apache_beam.options.pipeline_options import PipelineOptions

class InsertToPostgres(beam.DoFn):
    def __init__(self, connection_string):
        self.connection_string = connection_string

    def process(self, element):
        try:
            connection = psycopg2.connect(self.connection_string)
            cursor = connection.cursor()

            insert_query = "INSERT INTO fifteen_minutes (datetime, street_id, count, velocity) VALUES (%s, %s, %s, %s)"

            for item in element:
                # Convert the street_time value to an integer
                street_time = int(round(float(item['street_id'])))
                values = (item['datetime'], street_time, item['count'], item['velocity'])
                cursor.execute(insert_query, values)
                connection.commit()
        except Exception as e:
            print(f"Error inserting data: {e}")
        finally:
            cursor.close()
            connection.close()

class CallAPI(beam.DoFn):
    def process(self, element):
        fro = element.get("from")
        to = element.get("to")

        url = 'http://localhost:8000/content?from={}&to={}'.format(fro, to)
        res = requests.get(url)
        data = res.json()
        for item in data:
            yield item

def run(from_value, to_value):
    pipeline_options = PipelineOptions()

    with beam.Pipeline(options=pipeline_options) as p:
        data = p | beam.Create(values=[{'from': from_value, 'to': to_value}])
        connection_string = "dbname=test_db user=postgres password=postgres host=localhost port=5432"

        api_response = data | "Call API" >> beam.ParDo(CallAPI())
        api_response | "Insert to PostgreSQL" >> beam.ParDo(InsertToPostgres(connection_string))




# In[ ]:





# In[ ]:




