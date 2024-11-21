import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import time
from datetime import datetime
import pickle
import requests

from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

class DV360DataFetcher:
    def __init__(self, credentials_path):
        self.SCOPES = [
            'https://www.googleapis.com/auth/doubleclickbidmanager',
            'https://www.googleapis.com/auth/spreadsheets'
        ]
        self.credentials_path = credentials_path
        self.creds = None

    def authenticate(self):
        """Handle authentication"""
        try:
            if os.path.exists('token.pickle'):
                with open('token.pickle', 'rb') as token:
                    self.creds = pickle.load(token)

            if not self.creds or not self.creds.valid:
                if self.creds and self.creds.expired and self.creds.refresh_token:
                    self.creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_path, self.SCOPES)
                    self.creds = flow.run_local_server(port=8080)

                with open('token.pickle', 'wb') as token:
                    pickle.dump(self.creds, token)

            self.dbm_service = build(
                'doubleclickbidmanager',
                'v2',
                credentials=self.creds
            )
            
            self.sheets_service = build(
                'sheets',
                'v4',
                credentials=self.creds
            )
            
            print("Successfully authenticated and built services")
            return True

        except Exception as e:
            print(f"Authentication error: {str(e)}")
            return False

    def create_query(self, advertiser_id):
        """Create a DV360 report query with correct v2 format"""
        try:
            # query_body = {
            #     "metadata": {
            #         "title": f"DV360 Report {datetime.now().strftime('%Y%m%d')}",
            #         "dataRange": "LAST_30_DAYS",  # Adjust to a supported range as needed
            #         "format": "CSV"
            #     },
            #     "params": {
            #         "metrics": [
            #             "impressions",
            #             "clicks",
            #             "totalMediaCostAdvertiser",
            #             "totalConversions"
            #         ],
            #         "groupBys": [
            #             "date",
            #             "advertiser",
            #             "insertionOrder",
            #             "lineItem",
            #             "creativeId"
            #         ]
            #     }
            # }

            display_name = 'DV 360 Report for testing.'

            # Advertiser ID and campaign ID by which to filter report data.
            advertiser_id = advertiser_id
            campaign_id = '55089520'

            # Build query object with basic dimension and metrics values.
            query_obj = {
                "metadata": {
                    "title": display_name,
                    "dataRange": {"range": "LAST_7_DAYS"},
                    "format": "CSV",
                },
                "params": {
                    "type": "STANDARD",
                    "groupBys": [
                        "FILTER_ADVERTISER_NAME",
                        "FILTER_ADVERTISER",
                        "FILTER_ADVERTISER_CURRENCY",
                        "FILTER_INSERTION_ORDER_NAME",
                        "FILTER_INSERTION_ORDER",
                        "FILTER_LINE_ITEM_NAME",
                        "FILTER_LINE_ITEM",
                    ],
                    "filters": [
                        {"type": "FILTER_ADVERTISER", "value": advertiser_id},
                        {"type": "FILTER_MEDIA_PLAN", "value": campaign_id}

                    ],
                    "metrics": [
                        "METRIC_IMPRESSIONS",
                        "METRIC_BILLABLE_IMPRESSIONS",
                        "METRIC_CLICKS",
                        "METRIC_CTR",
                        "METRIC_TOTAL_CONVERSIONS",
                        "METRIC_LAST_CLICKS",
                        "METRIC_LAST_IMPRESSIONS",
                        "METRIC_REVENUE_ADVERTISER",
                        "METRIC_MEDIA_COST_ADVERTISER",
                    ],
                },
                "schedule": {"frequency": "ONE_TIME"}
            }

            # Create query object.
            #query_response = service.queries().create(body=query_obj).execute()

            # Print new query ID.
            #print(f'Query {query_response["queryId"]} was created.')

            print("Creating query with body:", query_obj)
            response = self.dbm_service.queries().create(body=query_obj).execute()
            print("Query response:", response)

            queryId = response.get('queryId')
            query = self.dbm_service.queries().get(queryId=queryId).execute()
            print(
                f'Query ID {queryId} retrieved with display name'
                f' {query["metadata"]["title"]}.'
            )
            return response.get('queryId')

        except HttpError as e:
            print(f"HTTP Error during query creation: {e.content}")
            return None
        except Exception as e:
            print(f"Error creating query: {e}")
            return None

    def wait_for_report(self, query_id):
        """Wait for report to be generated"""
        max_attempts = 10
        attempts = 0
        
        while attempts < max_attempts:
            try:
                print(f"Checking report status (Attempt {attempts + 1}/{max_attempts})")
                query = self.dbm_service.queries().get(queryId=query_id).execute()
                # reportId = query.get('reportId')
                # print('report id==>',reportId)
                if not query.get('metadata', {}).get('running', False):
                    print("Report generation completed")
                    return True
                    
                print("Report still generating, waiting 30 seconds...")
                time.sleep(30)
                attempts += 1
                
            except Exception as e:
                print(f"Error checking report status: {e}")
                return False
                
        print("Report generation timed out")
        return False

    # def get_report_data(self, query_id):
    #     """Get report data"""
    #     try:
    #         print("Retrieving report data...")
    #         print('Query id in report data->',query_id)
    #         response = self.dbm_service.queries().reports().list(queryId=query_id).execute()
            
    #        # response = self.dbm_service.queries().reports().list(queryId=query_id).execute()
        
    #         # if 'reports' in response:
    #         #     for report in response['reports']:
    #         #         print(f"Report ID: {report['key']['reportId']}")
    #         #         print(f"Report Metadata: {report}")

    #         if not response.get('reports'):
    #             print("No reports found")
    #             return None

    #         latest_report = response['reports'][-1]
    #         if 'metadata' in latest_report and 'googleCloudStoragePath' in latest_report['metadata']:
    #             url = latest_report['metadata']['googleCloudStoragePath']
    #             print(f"Downloading report from: {url}")
    #             df = pd.read_csv(url)
    #             return df
                
    #         print("No report URL found")
    #         return None
            
    #     except Exception as e:
    #         print(f"Error getting report data: {e}")
    #         return None

    def get_report_data(self, query_id, max_retries=5, wait_time=30):
        """
        Get report data for a given query_id.
        Retries until the report is available or max_retries is reached.
        """
        try:
            print("Retrieving report data...")
            print(f"Query ID in report data: {query_id}")

            retries = 0

            while retries < max_retries:
                try:
                    # Fetch reports for the given query ID
                    response = self.dbm_service.queries().reports().list(queryId=query_id).execute()
                    
                    if not response.get('reports'):
                        print("No reports found. Retrying...")
                        retries += 1
                        time.sleep(wait_time)
                        continue

                    # Get the latest report
                    latest_report = response['reports'][-1]
                    if 'metadata' in latest_report and 'googleCloudStoragePath' in latest_report['metadata']:
                        url = latest_report['metadata']['googleCloudStoragePath']
                        print(f"Downloading report from: {url}")
                        df = pd.read_csv(url)  # Download and parse the report as a DataFrame
                        return df

                    print("No report URL found. Retrying...")
                    retries += 1
                    time.sleep(wait_time)

                except HttpError as http_err:
                    print(f"HTTP Error: {http_err}")
                    retries += 1
                    time.sleep(wait_time)
                except Exception as e:
                    print(f"Error fetching report: {e}")
                    retries += 1
                    time.sleep(wait_time)

            print("Max retries reached. Report not available.")
            return None

        except Exception as e:
            print(f"Error getting report data: {e}")
            return None

    def update_sheet(self, spreadsheet_id, data, sheet_name='DV360 Data'):
        """Update Google Sheet with report data"""
        try:
            print(f"Updating sheet: {sheet_name}")
            
            # Create new sheet if it doesn't exist
            spreadsheet = self.sheets_service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            
            sheet_exists = False
            for sheet in spreadsheet.get('sheets', []):
                if sheet['properties']['title'] == sheet_name:
                    sheet_exists = True
                    break
                    
            if not sheet_exists:
                print(f"Creating new sheet: {sheet_name}")
                request = {
                    'addSheet': {
                        'properties': {
                            'title': sheet_name
                        }
                    }
                }
                self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'requests': [request]}
                ).execute()

            # Prepare data for upload
            values = [data.columns.values.tolist()] + data.values.tolist()
            body = {'values': values}

            # Clear existing content
            self.sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:ZZ"
            ).execute()

            # Update with new data
            result = self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption='RAW',
                body=body
            ).execute()

            print(f"Updated {result.get('updatedCells')} cells")
            return True

        except Exception as e:
            print(f"Error updating sheet: {e}")
            return False
        
    def fetch_report(self,query_id):
        query_results = self.dbm_service.queries().get(queryId=query_id).execute()
        report_url = query_results["metadata"].get("googleCloudStoragePathForLatestReport")
        if report_url:
            print(f"Report URL: {report_url}")
            # Download the file
            response = requests.get(report_url)
            with open("campaign_report.csv", "wb") as file:
                file.write(response.content)
            print("Report downloaded.")
        else:
            print("Report not yet available. Try again later.")


def main():
    
    print('Creating In main')
    # Configuration
    CREDENTIALS_PATH = os.getenv('CREDENTIALS_PATH') # Update with your credentials file path
    SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')  # Update with your spreadsheet ID
    ADVERTISER_ID = os.getenv('ADVERTISER_ID')  # Your advertiser ID
    SHEET_NAME = os.getenv('SHEET_NAME')  # Name of the sheet to update

    # Initialize fetcher
    fetcher = DV360DataFetcher(CREDENTIALS_PATH)
    
    print("\nStarting DV360 data fetch process...")
    
    # Authenticate
    if not fetcher.authenticate():
        print("Authentication failed!")
        return

    # Create query
    print("\nCreating DV360 report query...")
    #query_id = fetcher.create_query(ADVERTISER_ID)
    query_id = '1356166266'
    # if not query_id:
    #     print("Failed to create query!")
    #     return

    print(f"\nQuery created successfully with ID: {query_id}")

    # Wait for report
    print("\nWaiting for report generation...")
    # if not fetcher.wait_for_report(query_id):
    #     print("Report generation failed or timed out!")
    #     return

    # Get report data
    print("\nFetching report data...")
    data = fetcher.get_report_data(query_id)
    res=fetcher.fetch_report(query_id)
    
if __name__ == '__main__':
    main()
