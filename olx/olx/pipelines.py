# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import datetime

# useful for handling different item types with a single interface
import json
import os

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2 import service_account

from .settings import GOOGLE_APPLICATION_CREDENTIALS_TABLETOP_BIGQUERY_WORKER_JSON, BIGQUERY_EXPORT_PIPEPLINE_DATASET_ID, BIGQUERY_EXPORT_PIPELINE_TABLE_ID

class BigQueryExportPipeline:
    credentials_raw = GOOGLE_APPLICATION_CREDENTIALS_TABLETOP_BIGQUERY_WORKER_JSON
    # Your dataset and table name in BigQuery
    dataset_id = BIGQUERY_EXPORT_PIPEPLINE_DATASET_ID
    table_id = BIGQUERY_EXPORT_PIPELINE_TABLE_ID

    def open_spider(self, spider):
        credentials = service_account.Credentials.from_service_account_file(
            self.credentials_raw
        )

        # Initialize BigQuery client
        self.client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )
        # Ensure the table exists
        self.ensure_table_exists(spider)

        # List to store items
        self.items = []


    def ensure_table_exists(self, spider):
        # Get dataset reference
        dataset_ref = self.client.dataset(self.dataset_id)

        # Get table reference
        table_ref = dataset_ref.table(self.table_id)

        try:
            # Check if the table exists
            self.client.get_table(table_ref)
            spider.logger.info(
                f"Table {self.table_id} exists in dataset {self.dataset_id}."
            )
        except NotFound:
            # If table not found, create it
            spider.logger.info(
                f"Table {self.table_id} not found in dataset {self.dataset_id}, creating..."
            )

            # Define your table schema
            schema = [
                bigquery.SchemaField("ad_id", "STRING"),
                bigquery.SchemaField("scraped_date", "DATE"),
                bigquery.SchemaField("scraped_timestamp", "TIMESTAMP"),
                bigquery.SchemaField("published", "TIMESTAMP"),
                bigquery.SchemaField("category", "STRING"),
                bigquery.SchemaField("label", "STRING"),
                bigquery.SchemaField("title", "STRING"),
                bigquery.SchemaField("price", "FLOAT"),
                bigquery.SchemaField("status", "INT64"),
                bigquery.SchemaField("city", "STRING"),
                bigquery.SchemaField("district", "STRING"),
                bigquery.SchemaField("url", "STRING"),
            ]

            # Create table
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            spider.logger.info(f"Table {self.table_id} created.")

    def process_item(self, item, spider):
        # Add the current item to our list of items.
        # Ensure all data is in a format suitable for your BigQuery table schema
        self.items.append(
            {
                "ad_id": item.get("ad_id"),
                "scraped_date": item.get("scraped_date"),
                "scraped_timestamp": item.get("scraped_timestamp"),
                "published": item.get("published"),
                "category": item.get("category"),
                "label": item.get("label"),
                "title": item.get("title"),
                "price": item.get("price"),
                "status": item.get("status"),
                "city": item.get("city"),
                "district": item.get("district"),
                "url": item.get("url"),
            }
        )
        return item

    def close_spider(self, spider):
        if not self.items:
            spider.logger.warning("No items to upload to BigQuery.")
            return

        # Get table reference
        table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
        table = self.client.get_table(table_ref)

        # Insert data into BigQuery
        errors = self.client.insert_rows_json(table, self.items)

        # Log errors if they occur
        if errors:
            spider.logger.error(errors)
