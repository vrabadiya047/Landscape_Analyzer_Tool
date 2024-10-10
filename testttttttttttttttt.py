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


        def parse_systems_xml(xml_content):
            root = ET.fromstring(xml_content)
            systems_data = []
            for item in root.findall('.//item'):
                systems_data.append({
                    "sid": item.find('SID').text if item.find('SID') is not None else '',
                    "product_version": item.find('PRODUCTVERSION').text if item.find(
                        'PRODUCTVERSION') is not None else '',
                    "description": item.find('DESCRIPTION').text if item.find('DESCRIPTION') is not None else '',
                    "installation_number": item.find('INSTALLTION_NUMBER').text if item.find(
                        'INSTALLTION_NUMBER') is not None else '',
                    "transfer_domain": item.find('TRANSFER_DOMAIN').text if item.find(
                        'TRANSFER_DOMAIN') is not None else '',
                    "host_ip": item.find('HOST_IP').text if item.find('HOST_IP') is not None else '',
                    "host_name": item.find('HOST_NAME').text if item.find('HOST_NAME') is not None else ''
                })
            return systems_data

        def parse_components_xml(xml_content):
            root = ET.fromstring(xml_content)
            components_data = []
            for item in root.findall('.//item'):
                components_data.append({
                    "sid": item.find('SID').text if item.find('SID') is not None else '',
                    "product_version": item.find('PRODUCTVERSION').text if item.find(
                        'PRODUCTVERSION') is not None else '',
                    "name": item.find('NAME').text if item.find('NAME') is not None else '',
                    "release": item.find('RELEASE').text if item.find('RELEASE') is not None else '',
                    "patch_level": item.find('PATCHLEVEL').text if item.find('PATCHLEVEL') is not None else '',
                    "description": item.find('DESCRIPTION').text if item.find('DESCRIPTION') is not None else '',
                    "type": item.find('TYPE').text if item.find('TYPE') is not None else '',
                    "category": item.find('CATEGORY').text if item.find('CATEGORY') is not None else ''
                })
            return components_data

        def parse_Hosts_xml(xml_content):
            root = ET.fromstring(xml_content)
            hosts_data = []
            for item in root.findall('.//item'):
                hosts_data.append({
                    "sid": item.find('SID').text if item.find('SID') is not None else '',
                    "product_version": item.find('PRODUCTVERSION').text if item.find(
                        'PRODUCTVERSION') is not None else '',
                    "name": item.find('NAME').text if item.find('NAME') is not None else '',
                    "category": item.find('CATEGORY').text if item.find('CATEGORY') is not None else '',
                    "ip_address": item.find('IP_ADDRESS').text if item.find('IP_ADDRESS') is not None else '',
                    "cpu_type": item.find('CPU_TYPE').text.strip() if item.find('CPU_TYPE') is not None and item.find(
                        'CPU_TYPE').text is not None else '',
                    "cpu_frequency": item.find('CPU_FREQUENCY').text.strip() if item.find(
                        'CPU_FREQUENCY') is not None and item.find('CPU_FREQUENCY').text is not None else '',
                    "memory": item.find('MEMORY').text.strip() if item.find('MEMORY') is not None and item.find(
                        'MEMORY').text is not None else ''
                })
            return hosts_data

        def parse_clients_xml(xml_content):
            root = ET.fromstring(xml_content)
            clients_data = []
            for item in root.findall('.//item'):
                clients_data.append({
                    "sid": item.find('SID').text if item.find('SID') is not None else '',
                    "product_version": item.find('PRODUCTVERSION').text if item.find(
                        'PRODUCTVERSION') is not None else '',
                    "client_id": item.find('CLIENT_ID').text if item.find('CLIENT_ID') is not None else '',
                    "description": item.find('DESCRIPTION').text if item.find('DESCRIPTION') is not None else '',
                    "client_role": item.find('CLIENT_ROLE').text if item.find('CLIENT_ROLE') is not None else '',
                    "logical_system_name": item.find('LOGICALSYSTEMNAME').text if item.find(
                        'LOGICALSYSTEMNAME') is not None else ''
                })
            return clients_data

        def parse_modules_xml(xml_content):
            root = ET.fromstring(xml_content)
            systems_data = []
            for item in root.findall('.//item'):
                systems_data.append({
                    "sid": item.find('SID').text if item.find('SID') is not None else '',
                    "product_version": item.find('PRODUCTVERSION').text if item.find(
                        'PRODUCTVERSION') is not None else '',
                    "Client_id": item.find('CLIENT_ID').text if item.find('CLIENT_ID') is not None else '',
                    "belongs_to": item.find('BELONGSTO').text if item.find(
                        'BELONGSTO') is not None else '',
                    "acronym": item.find('ACRONYM').text if item.find('ACRONYM') is not None else '',
                    "name": item.find('NAME').text if item.find('NAME') is not None else '',
                    "category": item.find('CATEGORY').text if item.find('CATEGORY') is not None else ''
                })
            return systems_data

        def parse_Ale_xml(xml_content):
            root = ET.fromstring(xml_content)
            ale_data = []
            for item in root.findall('.//item'):
                ale_data.append({
                    "sid": item.find('SID').text if item.find('SID') is not None else '',
                    "product_version": item.find('PRODUCTVERSION').text if item.find(
                        'PRODUCTVERSION') is not None else '',
                    "sender": item.find('SENDER').text if item.find('SENDER') is not None else '',
                    "receiver": item.find('RECEIVER').text if item.find('RECEIVER') is not None else '',
                    "model": item.find('MODEL').text if item.find('MODEL') is not None else '',
                    "idoc_messagetype": item.find('IDOC_MESSAGETYPE').text if item.find(
                        'IDOC_MESSAGETYPE') is not None else '',
                    "filter_object": item.find('FILTEROBJCT').text if item.find('FILTEROBJCT') is not None else '',
                    "object": item.find('OBJECT').text if item.find('OBJECT') is not None else ''
                })
            return ale_data

        def parse_Rfc_xml(xml_content):
            root = ET.fromstring(xml_content)
            rfc_data = []
            for item in root.findall('.//item'):
                rfc_data.append({
                    "sid": item.find('SID').text if item.find('SID') is not None else '',
                    "product_version": item.find('PRODUCTVERSION').text if item.find(
                        'PRODUCTVERSION') is not None else '',
                    "rfc_client": item.find('RFC_CLIENT').text if item.find('RFC_CLIENT') is not None else '',
                    "rfc_destination": item.find('RFC_DESTINATION').text if item.find(
                        'RFC_DESTINATION') is not None else '',
                    "rfc_type": item.find('RFC_TYPE').text if item.find('RFC_TYPE') is not None else '',
                    "asynchronous": item.find('ASYNCHRONOUS').text if item.find('ASYNCHRONOUS') is not None else '',
                    "target": item.find('TARGET').text if item.find('TARGET') is not None else '',
                    "system_number": item.find('SYSTEMNUMBER').text if item.find('SYSTEMNUMBER') is not None else '',
                    "rfc_user": item.find('RFC_USER').text if item.find('RFC_USER') is not None else '',
                    "rfc_password": item.find('RFC_PASSWORD').text if item.find('RFC_PASSWORD') is not None else '',
                    "rfc_description": item.find('RFC_DESCRIPTION').text if item.find(
                        'RFC_DESCRIPTION') is not None else ''
                })
            return rfc_data



        System_xml_path = os.path.join(xml_directory, 'Systems.xml')
        components_xml_path = os.path.join(xml_directory, 'Components.xml')
        Hosts_xml_path = os.path.join(xml_directory, 'Hosts.xml')
        Clients_xml_path = os.path.join(xml_directory, 'Clients.xml')
        modules_xml_path = os.path.join(xml_directory, 'modules.xml')
        Ale_xml_path = os.path.join(xml_directory, 'ALE.xml')
        Rfc_xml_path = os.path.join(xml_directory, 'Rfc.xml')

        # Read XML content
        System_xml_content = read_xml_file(System_xml_path)
        components_xml_content = read_xml_file(components_xml_path)
        Hosts_xml_content = read_xml_file(Hosts_xml_path)
        Clients_xml_content = read_xml_file(Clients_xml_path)
        modules_xml_content = read_xml_file(modules_xml_path)
        Ale_xml_content = read_xml_file(Ale_xml_path)
        Rfc_xml_content = read_xml_file(Rfc_xml_path)
        idoc_data = load_data_object_excel().to_dict(orient='records')

        systems_data = parse_systems_xml(System_xml_content)
        components_data = parse_components_xml(components_xml_content)
        hosts_data = parse_Hosts_xml(Hosts_xml_content)
        clients_data = parse_clients_xml(Clients_xml_content)
        modules_data = parse_modules_xml(modules_xml_content)
        Ale_data = parse_Ale_xml(Ale_xml_content)
        Rfc_data = parse_Rfc_xml(Rfc_xml_content)

        if create_factsheet:
            os.makedirs(operation_folder, exist_ok=True)
            excel_file_path = os.path.join(operation_folder, 'created_factsheets.xlsx')

            try:
                for _ in range(3):  # Loop to run the process three times
                    # Check if user canceled before starting
                    if user_cancel:
                        print("User canceled operation before starting. Saving logs up to this point...")
                        save_factsheet_logs_to_excel(logs)
                        return excel_file_path

                    # Fetch existing applications from LeanIX
                    application_query = """
                        {
                          allFactSheets(factSheetType: Application) {
                            edges {
                              node {
                                id
                                displayName
                                name
                                description
                                tags {
                                  name
                                }
                              }
                            }
                          }
                        }"""

                    response2 = requests.post(url=request_url, headers=headers, json={"query": application_query})
                    response2.raise_for_status()
                    existing_applications = response2.json()['data']['allFactSheets']['edges']

                    # Normalize function for names
                    def normalize_name(name):
                        return unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii').lower()

                    # Normalize application names for comparison
                    existing_app_names = {normalize_name(app['node']['displayName']): app['node'] for app in
                                          existing_applications}

                    # Create new applications if they don't exist
                    xml_applications = {f"{system['sid']} {system['product_version']}": system['description'] for system
                                        in systems_data}
                    normalized_xml_applications = {normalize_name(app): app for app in xml_applications}
                    applications_to_create = {app: xml_applications[app] for norm_app, app in
                                              normalized_xml_applications.items() if norm_app not in existing_app_names}
                    application_level1 = []

                    for app_name in applications_to_create:
                        # Check if user canceled during the loop
                        if user_cancel:
                            print("User canceled operation. Saving logs up to this point...")
                            save_factsheet_logs_to_excel(logs)
                            return excel_file_path

                        mutation = """
                           mutation ($input: BaseFactSheetInput!, $patches: [Patch]!) {
                               createFactSheet(input: $input, patches: $patches) {
                                   factSheet {
                                       id
                                       name
                                       description
                                       type
                                       tags {
                                           id
                                           name
                                       }
                                   }
                               }
                           }
                           """
                        variables = {
                            "input": {
                                "name": app_name,
                                "type": "Application"
                            },
                            "patches": [
                                {
                                    "op": "add",
                                    "path": "/tags",
                                    "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'
                                }
                            ]
                        }
                        data = {"query": mutation, "variables": variables}
                        response21 = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                        response21.raise_for_status()
                        response_data_create = response21.json()
                        application_id = response_data_create['data']['createFactSheet']['factSheet']['id']
                        application_level1.append(application_id)

                        # Log the creation action
                        logs['Application'].append({
                            'Action': 'Created',
                            'Name': app_name,
                            'FactSheet Type': 'Application',
                            'ID': application_id  # Include the ID in the log
                        })

                        print(f"Application factsheet {app_name} created")

                    # Save logs after processing applications
                    save_factsheet_logs_to_excel(logs)

                    # Check for user cancellation after applications creation
                    if user_cancel:
                        print("User canceled operation after creating applications. Saving logs up to this point...")
                        return excel_file_path

                    # Fetch existing IT components from LeanIX
                    component_query = """
                            {
                              allFactSheets(factSheetType: ITComponent) {
                                edges {
                                  node {
                                    id
                                    name
                                    description
                                    ... on ITComponent {
                                          release
                                        }
                                  }
                                }
                              }
                            }
                            """
                    response3 = requests.post(url=request_url, headers=headers, json={"query": component_query})
                    response3.raise_for_status()
                    leanix_it_components = [node['node']['name'] for node in
                                            response3.json()['data']['allFactSheets']['edges']]

                    # Compare and Identify Missing IT Components
                    missing_it_components = [component for component in components_data if
                                             component['name'] not in leanix_it_components]

                    # Create missing IT Components
                    for component in missing_it_components:
                        # Check if user canceled during the loop
                        if user_cancel:
                            print("User canceled operation. Saving logs up to this point...")
                            save_factsheet_logs_to_excel(logs)
                            return excel_file_path

                        name1 = component['name']
                        description1 = component['description']
                        release1 = component['release']

                        mutation = """
                            mutation ($input: BaseFactSheetInput!, $patches: [Patch]!) {
                                createFactSheet(input: $input, patches: $patches) {
                                    factSheet {
                                        id
                                        name
                                        type
                                        description
                                        category
                                        tags {
                                            id
                                            name
                                        }
                                        ... on ITComponent {
                                            release
                                        }
                                    }
                                }
                            }
                        """
                        variables = {
                            "input": {
                                "name": name1,
                                "type": "ITComponent"
                            },
                            "patches": [
                                {
                                    "op": "add",
                                    "path": "/description",
                                    "value": description1
                                },
                                {
                                    "op": "add",
                                    "path": "/category",
                                    "value": "software"
                                },
                                {
                                    "op": "add",
                                    "path": "/tags",
                                    "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'
                                },
                                {
                                    "op": "add",
                                    "path": "/release",
                                    "value": release1
                                }
                            ]
                        }

                        data = {"query": mutation, "variables": variables}
                        response5 = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                        response5.raise_for_status()
                        response_data_create = response5.json()

                        # Check if the response contains errors
                        if 'errors' in response_data_create:
                            print(f"Error creating IT Component factsheet: {response_data_create['errors']}")
                            continue  # Skip to the next component

                        # Extract the ID if present
                        try:
                            it_component_id = response_data_create['data']['createFactSheet']['factSheet']['id']

                            # Log the creation action with ID
                            logs['ITComponent'].append({
                                'Name': name1,
                                'Action': 'Created',
                                'FactSheet Type': 'ITComponent',
                                'ID': it_component_id  # Include the ID in the log
                            })

                            print(f"IT Component factsheet {name1} created")
                        except KeyError:
                            print("Unexpected response structure. Could not find the 'id' in the response.")

                    # Save logs after processing IT components
                    save_factsheet_logs_to_excel(logs)

                    # Check if user canceled after processing IT components
                    if user_cancel:
                        print("User canceled operation. Saving logs up to this point...")
                        return excel_file_path



            except Exception as e:
                print(f"An error occurred: {e}")
                save_factsheet_logs_to_excel(logs)
                raise

            # After creating all applications and IT Components, save the logs to an Excel file
            file_path5 = save_factsheet_logs_to_excel(logs)
            return file_path5



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
