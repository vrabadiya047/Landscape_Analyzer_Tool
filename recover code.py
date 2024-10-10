import os
import requests
import json
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as ET
import unicodedata
from difflib import SequenceMatcher


def read_xml_file(file_path):
    with open(file_path, 'rb') as file:
        xml_content = file.read().decode('utf-16')
    return xml_content


def load_data_object_excel():
    excel_data = pd.read_excel('data_object.xlsx')
    return excel_data


def process_system_analysis(auth_url, request_url, api_token, xml_directory, output_base_dir, tag_name,
                            create_factsheet, delta_operation, test_mode, delete_operation, tag_name_2,
                            user_cancel=False, recovery=False, selected_version=None):
    try:
        # Perform the authentication to get the access token
        response = requests.post(auth_url, auth=('apitoken', api_token), data={'grant_type': 'client_credentials'})
        response.raise_for_status()
        access_token = response.json()['access_token']
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

        output_base_dir2 = 'path_to_output_directory'  # Ensure this is the correct path to the directory
        if output_base_dir2 is None or not os.path.isdir(output_base_dir2):
            raise ValueError(f"Invalid output_base_dir2 path: {output_base_dir2}")

        # Initialize log structure
        logs = {
            'Application': [],
            'ITComponent': [],
            'TechnicalStack': [],
            'BusinessCapability': [],
            'DataObject': [],
            'Interface': []
        }

        deleted_factsheet_logs = []  # To track deleted factsheets
        output_base_dir2 = 'path_to_output_directory'
        # Create folder with timestamp at the start of the operation
        timestamp = datetime.now().strftime('%d-%m-%Y - %H-%M')
        operation_folder = os.path.join(output_base_dir2, timestamp)
        excel_file_path = os.path.join(operation_folder, 'created_factsheets.xlsx')



        def save_factsheet_logs_to_excel(logss):
            """
            Save or append logs to the Excel file during the process.
            """
            combined_logs = []
            for factsheet_type, log_entries in logss.items():
                for entry in log_entries:
                    combined_entry = entry.copy()
                    combined_entry['FactSheet Type'] = factsheet_type
                    combined_logs.append(combined_entry)

            combined_df = pd.DataFrame(combined_logs)

            # Ensure the DataFrame has 'Description' column
            if 'Description' not in combined_df.columns:
                combined_df['Description'] = None

            # Remove duplicates where both 'Name' and 'FactSheet Type' are the same
            combined_df.drop_duplicates(subset=['Name', 'FactSheet Type'], keep='first', inplace=True)

            # Check if the file exists, and append if it does
            if os.path.exists(excel_file_path):
                with pd.ExcelWriter(excel_file_path, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                    combined_df.to_excel(writer, sheet_name='Logs', index=False)
            else:
                with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
                    combined_df.to_excel(writer, sheet_name='Logs', index=False)

        def save_logs_and_continue(exception=None):
            """
            Saves the logs of the deleted factsheets even if an error occurs.
            """
            print(f"Saving logs due to error: {exception}") if exception else None
            save_factsheet_logs_to_excel(logs)
            print(f"Logs saved to: {excel_file_path}")
            return excel_file_path

        # Recovery process
        # Recovery process

        if delete_operation:
            any_factsheets_deleted = False  # Flag to check if any fact sheets were deleted
            os.makedirs(operation_folder, exist_ok=True)
            excel_file_path = os.path.join(operation_folder, 'deleted_factsheets.xlsx')

            def fetch_all_fact_sheets(headers, request_url, tag_name, tag_name_2):
                fact_sheet_types = ["Application", "ITComponent", "TechnicalStack", "BusinessCapability", "DataObject",
                                    "Interface"]
                fact_sheets = {}

                for fact_sheet_type in fact_sheet_types:
                    query = f"""
                    query {{
                      allFactSheets(factSheetType: {fact_sheet_type}) {{
                        edges {{
                          node {{
                            id
                            name
                            tags {{
                              name
                            }}
                          }}
                        }}
                      }}
                    }}
                    """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    result = response.json()

                    # Filter fact sheets to ensure they have both tags
                    fact_sheets[fact_sheet_type] = {
                        edge['node']['name'].lower(): edge['node']['id']
                        for edge in result['data']['allFactSheets']['edges']
                        if {tag_name, tag_name_2}.issubset({tag['name'] for tag in edge['node']['tags']})
                    }

                return fact_sheets

            for run in range(3):  # Run the deletion operation three times

                try:
                    # Fetch all fact sheets with both tags
                    fact_sheets_with_tag = fetch_all_fact_sheets(headers, request_url, tag_name, tag_name_2)

                    # Convert the fetched fact sheets into the format expected by the archive function
                    fact_sheets_to_archive = [
                        {'id': fact_sheet_id, 'name': fact_sheet_name, 'type': fact_sheet_type}
                        for fact_sheet_type, fact_sheets in fact_sheets_with_tag.items()
                        for fact_sheet_name, fact_sheet_id in fact_sheets.items()
                    ]

                    if fact_sheets_to_archive:
                        any_factsheets_deleted = True  # Set the flag to True if any fact sheets are found to delete

                        # Backup the factsheets before deletion and save them to Excel
                        logs_before_deletion = {
                            fact_sheet['type']: [
                                {'ID': fact_sheet['id'], 'Name': fact_sheet['name'], 'Action': 'Backup'}]
                            for fact_sheet in fact_sheets_to_archive
                        }
                        #save_factsheet_logs_to_excel(logs_before_deletion)

                        # Log the deletion process for each fact sheet
                        for fact_sheet in fact_sheets_to_archive:
                            fact_sheet_type = fact_sheet['type']

                            try:
                                # If the user cancels, save logs and stop further processing
                                if user_cancel:
                                    print("User cancelled operation.")
                                    save_factsheet_logs_to_excel(logs)
                                    return excel_file_path

                                # Attempt to archive the fact sheets
                                archive_fact_sheets_without_ui(request_url, headers, [fact_sheet])

                                # Log deleted fact sheets
                                logs[fact_sheet_type].append({
                                    'Name': fact_sheet['name'],
                                    'Action': 'Deleted',
                                    'ID': fact_sheet['id'],
                                })
                                deleted_factsheet_logs.append({
                                    'Name': fact_sheet['name'],
                                    'FactSheet Type': fact_sheet_type,
                                    'Action': 'Deleted'
                                })

                                # Save logs to Excel after each deletion
                                save_factsheet_logs_to_excel(logs)


                            except Exception as e:
                                # If an error occurs, save the logs up until this point
                                save_logs_and_continue(e)
                                raise  # Re-raise the exception to stop further processing

                        print(f"Run {run + 1}: {len(fact_sheets_to_archive)} fact sheets deleted.")

                    else:
                        print(f"Run {run + 1}: No fact sheets found with both tags '{tag_name}' and '{tag_name_2}'.")

                except Exception as e:
                    # Save logs if any exception occurs during the deletion process
                    return save_logs_and_continue(e)

            # After all runs, save the logs to a single Excel file if any factsheets were deleted
            if any_factsheets_deleted:
                save_factsheet_logs_to_excel(logs)
                return excel_file_path

        if recovery:
            print("Recovery process initiated.")

            # Step 1: Unarchiving process from 'deleted_factsheets.xlsx'
            unarchive_successful = False
            if selected_version is not None:
                # Path to the selected version's directory
                version_folder = os.path.join(output_base_dir2, selected_version)
                deleted_factsheets_path = os.path.join(version_folder, 'deleted_factsheets.xlsx')

                if os.path.exists(deleted_factsheets_path):
                    print(f"'deleted_factsheets.xlsx' found at: {deleted_factsheets_path}")

                    # Load the Excel file
                    df = pd.read_excel(deleted_factsheets_path, engine='openpyxl')

                    # Check if the "ID" column exists
                    if 'ID' in df.columns:
                        print("ID column found. Attempting to unarchive the IDs:")
                        for fact_sheet_id in df['ID']:
                            # Unarchive the factsheet by setting its status to "ACTIVE"
                            try:
                                # Define the GraphQL mutation template
                                mutation_query = '''
                                        mutation ($patches: [Patch]!, $id: ID!) {
                                          updateFactSheet(
                                            id: $id
                                            comment: "Recover the application from Landscape Analyzer tool"
                                            patches: $patches
                                          ) {
                                            factSheet {
                                              id
                                              status
                                            }
                                          }
                                        }
                                        '''
                                # Define the variables for this ID
                                variables = {
                                    "id": fact_sheet_id,
                                    "patches": [
                                        {
                                            "op": "add",
                                            "path": "/status",
                                            "value": "ACTIVE"
                                        }
                                    ]
                                }
                                # Execute the mutation
                                response = requests.post(request_url, headers=headers,
                                                         json={'query': mutation_query, 'variables': variables})
                                response.raise_for_status()
                                result = response.json()
                                print(
                                    f"Successfully unarchived factsheet ID: {fact_sheet_id}, Status: {result['data']['updateFactSheet']['factSheet']['status']}")
                            except Exception as e:
                                print(f"Failed to unarchive factsheet ID: {fact_sheet_id} - Error: {e}")
                        unarchive_successful = True
                    else:
                        print("ID column not found in the Excel file.")
                else:
                    print("No Excel file named 'deleted_factsheets.xlsx' found in the selected version directory.")
            else:
                print("selected_version is None. Skipping unarchive task.")

            # Step 2: Check for 'created_factsheets.xlsx' in the same folder
            created_factsheets_path = os.path.join(version_folder, 'created_factsheets.xlsx')
            print(f"Checking for 'created_factsheets.xlsx' at: {created_factsheets_path}")
            if os.path.exists(created_factsheets_path):
                print(f"'created_factsheets.xlsx' found at: {created_factsheets_path}")
                created_df = pd.read_excel(created_factsheets_path, engine='openpyxl')

                # Extracting fact sheet information
                if 'ID' in created_df.columns:
                    print("ID column found. Attempting to archive the IDs:")
                    for fact_sheet_id in created_df['ID']:
                        try:
                            # Use the mutation template to archive the fact sheet
                            mutation = f"""
                                    mutation {{
                                      updateFactSheet(id: "{fact_sheet_id}", comment: "Archive  the application from Landscape Analyzer tool", 
                                        patches: {{op: replace, path: "/status", value: "ARCHIVED"}}, validateOnly: false) {{
                                        factSheet {{
                                          id
                                        }}
                                      }}
                                    }}
                                    """
                            response = requests.post(request_url, headers=headers, json={'query': mutation})
                            response.raise_for_status()
                            print(f"Successfully archived factsheet ID: {fact_sheet_id}")
                        except Exception as e:
                            print(f"Failed to archive factsheet ID: {fact_sheet_id} - Error: {e}")
                else:
                    print("ID column not found in 'created_factsheets.xlsx'.")
            else:
                print("No 'created_factsheets.xlsx' file found in the selected version directory.")

    except requests.RequestException as e:
        print(f"Request error: {e}")
        #save_logs_and_continue(e)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        #save_logs_and_continue(e)


def archive_fact_sheets_without_ui(request_url, headers, fact_sheets):
    mutation_template = """
    mutation {{
      updateFactSheet(id: "{id}", comment: "Archive", 
        patches: {{op: replace, path: "/status", value: "ARCHIVED"}}, validateOnly: false) {{
        factSheet {{
          id
        }}
      }}
    }}
    """
    for fact_sheet in fact_sheets:
        fact_sheet_type = fact_sheet['type']
        fact_sheet_name = fact_sheet['name']

        mutation = mutation_template.format(id=fact_sheet['id'])
        response = requests.post(request_url, headers=headers, json={'query': mutation})
        response.raise_for_status()
        response.json()

        print(f"{fact_sheet_type} fact sheet {fact_sheet_name} has been successfully deleted.")
