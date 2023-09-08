#!/usr/bin/env python
# coding: utf-8

# In[1]:


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_postgres import splitters
from beam_postgres.io import ReadFromPostgres
from datetime import datetime
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    '/MAWINGU/credentials/data-gearbox-396717-c956c18b8906.json',
    scopes=['https://www.googleapis.com/auth/bigquery']
)



class FilterWeekends(beam.DoFn):
    def process(self, element):
        dt = element['datetime']
        if dt.weekday() >= 5:
            yield element

class FilterWeekdays(beam.DoFn):
    def process(self, element):
        dt = element['datetime']
        if dt.weekday() < 5:
            yield element

class FormatData(beam.DoFn):
    def process(self, element):
        formatted_element = {
            'id': element['id'],
            'datetime': element['datetime'].strftime('%Y-%m-%d %H:%M:%S UTC'),
            'street_id': element['street_id'],
            'count': element['count'],
            'velocity': format(element['velocity'], '.2f')
        }
        yield formatted_element

def run():
    pipeline_options = PipelineOptions()

    with beam.Pipeline(options=pipeline_options) as p:
        read_from_postgres = ReadFromPostgres(
            query="SELECT * FROM public.fifteen_minutes;",
            host="localhost",
            database="test_db",
            user="postgres",
            password="postgres",
            splitter=splitters.NoSplitter()
        )

        data = p | "ReadFromPostgres" >> read_from_postgres

        weekend_data = data | "FilterWeekends" >> beam.ParDo(FilterWeekends())
        weekday_data = data | "FilterWeekdays" >> beam.ParDo(FilterWeekdays())

        # Format the data once
        formatted_data = data | "FormatData" >> beam.ParDo(FormatData())

        # Define BigQuery table references and schemas
        bq_table_weekend = 'data-gearbox-396717.transport_01.weekend_feb2019'  # Replace with actual IDs
        bq_schema_weekend = {
            'fields': [
                {'name': 'id', 'type': 'INTEGER'},
                {'name': 'datetime', 'type': 'TIMESTAMP'},
                {'name': 'street_id', 'type': 'INTEGER'},
                {'name': 'count', 'type': 'INTEGER'},
                {'name': 'velocity', 'type': 'FLOAT'},
            ]
        }

        bq_table_weekday = 'data-gearbox-396717.transport_01.weekday_feb2019'  # Replace with actual IDs
        bq_schema_weekday = {
            'fields': [
                {'name': 'id', 'type': 'INTEGER'},
                {'name': 'datetime', 'type': 'TIMESTAMP'},
                {'name': 'street_id', 'type': 'INTEGER'},
                {'name': 'count', 'type': 'INTEGER'},
                {'name': 'velocity', 'type': 'FLOAT'},
            ]
        }

        formatted_data | "WriteWeekendToBigQuery" >> beam.io.WriteToBigQuery(
            table=bq_table_weekend,
            schema=bq_schema_weekend,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location='gs://projectde_he/temp-location'
        )

        formatted_data | "WriteWeekdayToBigQuery" >> beam.io.WriteToBigQuery(
            table=bq_table_weekday,
            schema=bq_schema_weekday,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location='gs://projectde_he/temp-location'
        )

if __name__ == '__main__':
    run()




# In[ ]:




