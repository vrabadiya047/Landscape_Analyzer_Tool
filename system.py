import os
import time

import requests
import json
import xml.etree.ElementTree as ET
import unicodedata
from datetime import datetime
import pandas as pd
from difflib import SequenceMatcher


def read_xml_file(file_path):
    with open(file_path, 'rb') as file:
        xml_content = file.read().decode('utf-16')
    return xml_content


def load_data_object_excel():
    excel_data = pd.read_excel('data_object.xlsx')
    return excel_data


# #data_path = os.path.join(os.path.dirname(__file__), )


# Load the Excel data
file_path = 'data_object.xlsx'
excel_data = pd.read_excel(file_path)

# Extract the IDOC message types and their corresponding Langtext_EN
idoc_data = excel_data[['IDOC_Message_Type', 'Langtext_EN']].to_dict(orient='records')


def process_system_analysis(auth_url, request_url, api_token, xml_directory, output_base_dir, tag_name,
                            create_factsheet, delta_operation, test_mode, delete_operation, tag_name_2):
    # Perform the authentication to get the access token

    response = requests.post(auth_url, auth=('apitoken', api_token), data={'grant_type': 'client_credentials'})
    response.raise_for_status()
    access_token = response.json()['access_token']
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    # Define XML file paths
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


    deleted_factsheet_logs = []

    # Collect logs after archiving
    logs = {
        'Application': [],
        'ITComponent': [],
        'TechnicalStack': [],
        'BusinessCapability': [],
        'DataObject': [],
        'Interface': []
    }

    # Create Tags

    check_tag_query = """
    query {
      allTags {
        edges {
          node {
            id
            name
          }
        }
      }
    }
    """

    response2 = requests.post(request_url, headers=headers, json={"query": check_tag_query})
    response2.raise_for_status()
    response_data = response2.json()

    # Check if the response contains errors
    if 'errors' in response_data:
        print("Error in response:", response_data['errors'])
    else:
        tags = response_data.get('data', {}).get('allTags', {}).get('edges', [])

        # Check if the tags already exist
        tag_exists = any(tag['node']['name'] == tag_name for tag in tags)
        tag_2_exists = any(tag['node']['name'] == tag_name_2 for tag in tags)

        if not tag_exists:
            # Prepare the GraphQL Mutation to Create the first Tag
            create_tag_mutation = """
                mutation ($name: String!, $description: String, $color: String!, $tagGroupId: ID) {
                  createTag(
                    name: $name,
                    description: $description,
                    color: $color,
                    tagGroupId: $tagGroupId
                  ) {
                    id
                    name
                    description
                    color
                  }
                }
                """

            # Define variables for the mutation
            variables = {
                "name": tag_name,
                "description": "This is a Landscape Analyzer Tag",
                "color": "#3924ff",
            }

            # Send the Mutation Request to Create the first Tag
            response3 = requests.post(request_url, headers=headers,
                                      json={"query": create_tag_mutation, "variables": variables})
            response3.raise_for_status()

            # Print the response from the API
            print(f"Tag {tag_name} created.")

        if not tag_2_exists:
            # Prepare the GraphQL Mutation to Create the first Tag
            create_tag_mutation = """
                mutation ($name: String!, $description: String, $color: String!, $tagGroupId: ID) {
                  createTag(
                    name: $name,
                    description: $description,
                    color: $color,
                    tagGroupId: $tagGroupId
                  ) {
                    id
                    name
                    description
                    color
                  }
                }
                """

            # Define variables for the mutation
            variables = {
                "name": tag_name_2,
                "description": "This is a Landscape Analyzer Tag",
                "color": "#3924ff",
            }

            # Send the Mutation Request to Create the first Tag
            response3 = requests.post(request_url, headers=headers,
                                      json={"query": create_tag_mutation, "variables": variables})
            response3.raise_for_status()

            # Print the response from the API
            print(f"Tag {tag_name_2} created.")

    def save_factsheet_logs_to_excel(logs, file_prefix, test_mode=False):
        # Combine logs into a single DataFrame
        combined_logs = []
        for factsheet_type, log_entries in logs.items():
            for entry in log_entries:
                combined_entry = entry.copy()
                combined_entry['FactSheet Type'] = factsheet_type
                combined_logs.append(combined_entry)

        combined_df = pd.DataFrame(combined_logs)

        timestamp = datetime.now().strftime('%d-%m-%Y - %H-%M')

        if test_mode:
            # Create a hidden directory for test mode
            hidden_dir = os.path.join(os.path.expanduser('~'), '.hidden_test_mode_logs')
            os.makedirs(hidden_dir, exist_ok=True)

            file_path = os.path.join(hidden_dir, f"{file_prefix}.xlsx")

            # Save the file to the hidden directory
            with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
                combined_df.to_excel(writer, sheet_name='Logs', index=False)

            return file_path
        else:
            # Save to the path_to_output_directory in normal mode
            output_dir = os.path.join('path_to_output_directory', timestamp)
            os.makedirs(output_dir, exist_ok=True)
            file_path = os.path.join(output_dir, f"{file_prefix}.xlsx")

            # Save the DataFrame to the Excel file
            with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
                combined_df.to_excel(writer, sheet_name='Logs', index=False)

            return file_path  # Return the path to the Excel file

    def save_created_updated_factsheets_to_excel(created_factsheets, updated_factsheets, deleted_factsheets, duration):
        # Safeguard against None values in the created, updated, and deleted factsheets
        created_factsheets = [
            {k: (v if v is not None else '') for k, v in factsheet.items()}
            for factsheet in created_factsheets
        ]

        updated_factsheets = [
            {k: (v if v is not None else '') for k, v in factsheet.items()}
            for factsheet in updated_factsheets
        ]

        deleted_factsheets = [
            {k: (v if v is not None else '') for k, v in factsheet.items()}
            for factsheet in deleted_factsheets
        ]

        # Combine created, updated, and deleted factsheets into separate DataFrames
        created_df = pd.DataFrame(created_factsheets)
        updated_df = pd.DataFrame(updated_factsheets)
        deleted_df = pd.DataFrame(deleted_factsheets)

        timestamp = datetime.now().strftime('%d-%m-%Y - %H-%M')

        # Save to the path_to_output_directory in normal mode
        output_dir = os.path.join('path_to_output_directory', timestamp)
        os.makedirs(output_dir, exist_ok=True)

        created_file_path = os.path.join(output_dir, "created_factsheets.xlsx")
        updated_file_path = os.path.join(output_dir, "updated_factsheets.xlsx")
        deleted_file_path = os.path.join(output_dir, "deleted_factsheets.xlsx")

        # Add the duration as a column to the history DataFrame
        created_df['Duration'] = duration
        updated_df['Duration'] = duration
        deleted_df['Duration'] = duration

        # Save each DataFrame into its respective file
        with pd.ExcelWriter(created_file_path, engine='xlsxwriter') as writer:
            created_df.to_excel(writer, sheet_name='CreatedFactsheets', index=False)

        with pd.ExcelWriter(updated_file_path, engine='xlsxwriter') as writer:
            updated_df.to_excel(writer, sheet_name='UpdatedFactsheets', index=False)

        with pd.ExcelWriter(deleted_file_path, engine='xlsxwriter') as writer:
            deleted_df.to_excel(writer, sheet_name='DeletedFactsheets', index=False)

        return created_file_path, updated_file_path, deleted_file_path

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

    if delete_operation:
        start_time = time.time()
        any_factsheets_deleted = False  # Flag to check if any fact sheets were deleted

        for run in range(3):  # Run the deletion operation three times

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

                print(
                    f"Run {run + 1}: Found {len(fact_sheets_to_archive)} fact sheets with both tags '{tag_name}' and '{tag_name_2}'. Archiving them now.")
                archive_fact_sheets_without_ui(request_url, headers, fact_sheets_to_archive)

                for fact_sheet in fact_sheets_to_archive:
                    fact_sheet_type = fact_sheet['type']

                    logs[fact_sheet_type].append({
                        'Name': fact_sheet['name'],
                        'Action': 'Deleted'  # Set action to Deleted
                    })
                    deleted_factsheet_logs.append({
                        'Name': fact_sheet['name'],
                        'FactSheet Type': fact_sheet_type,
                        'Action': 'Deleted'
                    })
            else:
                print(f"Run {run + 1}: No fact sheets found with both tags '{tag_name}' and '{tag_name_2}'.")

        end_time = time.time()
        # Calculate the duration in seconds and format it in a readable format (e.g., HH:MM:SS)
        duration_seconds = end_time - start_time
        duration = time.strftime('%H:%M:%S', time.gmtime(duration_seconds))

        if any_factsheets_deleted:
            # Save the logs (Excel files) and include the duration
            file_path6 = save_factsheet_logs_to_excel(logs, "deleted_factsheets", test_mode=False)

            # Get the directory where the Excel files are saved (based on the timestamp)
            output_dir = os.path.dirname(file_path6)  # This will give us the folder where the Excel sheets are saved

            # Save the duration to a file inside the same output folder
            duration_file_path = os.path.join(output_dir, "duration.txt")
            with open(duration_file_path, 'w') as duration_file:
                duration_file.write(duration)

            # Now both the Excel sheets and the duration log will be in the same folder
            return file_path6  # You can return the duration file path if needed

    def parse_systems_xml(xml_content):
        root = ET.fromstring(xml_content)
        systems_data = []
        for item in root.findall('.//item'):
            systems_data.append({
                "sid": item.find('SID').text if item.find('SID') is not None else '',
                "product_version": item.find('PRODUCTVERSION').text if item.find('PRODUCTVERSION') is not None else '',
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
                "product_version": item.find('PRODUCTVERSION').text if item.find('PRODUCTVERSION') is not None else '',
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
                "product_version": item.find('PRODUCTVERSION').text if item.find('PRODUCTVERSION') is not None else '',
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
                "product_version": item.find('PRODUCTVERSION').text if item.find('PRODUCTVERSION') is not None else '',
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
                "product_version": item.find('PRODUCTVERSION').text if item.find('PRODUCTVERSION') is not None else '',
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
                "product_version": item.find('PRODUCTVERSION').text if item.find('PRODUCTVERSION') is not None else '',
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
                "product_version": item.find('PRODUCTVERSION').text if item.find('PRODUCTVERSION') is not None else '',
                "rfc_client": item.find('RFC_CLIENT').text if item.find('RFC_CLIENT') is not None else '',
                "rfc_destination": item.find('RFC_DESTINATION').text if item.find(
                    'RFC_DESTINATION') is not None else '',
                "rfc_type": item.find('RFC_TYPE').text if item.find('RFC_TYPE') is not None else '',
                "asynchronous": item.find('ASYNCHRONOUS').text if item.find('ASYNCHRONOUS') is not None else '',
                "target": item.find('TARGET').text if item.find('TARGET') is not None else '',
                "system_number": item.find('SYSTEMNUMBER').text if item.find('SYSTEMNUMBER') is not None else '',
                "rfc_user": item.find('RFC_USER').text if item.find('RFC_USER') is not None else '',
                "rfc_password": item.find('RFC_PASSWORD').text if item.find('RFC_PASSWORD') is not None else '',
                "rfc_description": item.find('RFC_DESCRIPTION').text if item.find('RFC_DESCRIPTION') is not None else ''
            })
        return rfc_data

        # Parse XML content

    systems_data = parse_systems_xml(System_xml_content)
    components_data = parse_components_xml(components_xml_content)
    hosts_data = parse_Hosts_xml(Hosts_xml_content)
    clients_data = parse_clients_xml(Clients_xml_content)
    modules_data = parse_modules_xml(modules_xml_content)
    Ale_data = parse_Ale_xml(Ale_xml_content)
    Rfc_data = parse_Rfc_xml(Rfc_xml_content)

    def log_and_save(log_type, log_entry):
        """
        Add a log entry to the specified log type and save the logs incrementally.
        """
        logs[log_type].append(log_entry)
        save_factsheet_logs_to_excel(logs)

        # Check if user canceled operation
        if user_cancel:
            print("User canceled operation. Saving logs up to this point...")
            raise KeyboardInterrupt("Operation canceled by the user.")


    if test_mode and delete_operation:
        any_factsheets_deleted = False  # Flag to check if any fact sheets were deleted

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

            print(
                f"Test Mode: Found {len(fact_sheets_to_archive)} fact sheets with both tags '{tag_name}' and '{tag_name_2}'.")

            for fact_sheet in fact_sheets_to_archive:
                fact_sheet_type = fact_sheet['type']
                print(f"Test mode: {fact_sheet_type} factsheet {fact_sheet['name']} has been deleted")

                logs[fact_sheet_type].append({
                    'Name': fact_sheet['name'],
                    'Action': 'Deleted'  # Set action to Deleted
                })
                deleted_factsheet_logs.append({
                    'Name': fact_sheet['name'],
                    'FactSheet Type': fact_sheet_type,
                    'Action': 'Deleted'
                })

        # After all three runs, save the logs to a single Excel file only if any fact sheets were deleted
        if any_factsheets_deleted:
            file_path3 = save_factsheet_logs_to_excel(logs, "deleted_factsheets", test_mode=True)
            return file_path3

    if test_mode and create_factsheet:
        print("Test Mode is ON. No actual import will be performed.")
        for run in range(3):  # Run the creation operation three times
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
            xml_applications = {f"{system['sid']} {system['product_version']}": system['description'] for system in
                                systems_data}
            normalized_xml_applications = {normalize_name(app): app for app in xml_applications}
            applications_to_create = {app: xml_applications[app] for norm_app, app in
                                      normalized_xml_applications.items() if
                                      norm_app not in existing_app_names}

            created_applications = [f"{system['sid']} {system['product_version']}" for system in systems_data if
                                    f"{system['sid']} {system['product_version']}" in applications_to_create]
            logs['Application'].extend(
                [{"Action": "Create", "FactSheet Type": "Application", "Name": app} for app in created_applications])
            for app in created_applications:
                print(f"Test Mode: Application factsheet {app} created")

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
            existing_it_components = response3.json()['data']['allFactSheets']['edges']
            existing_it_component_names = {normalize_name(component['node']['name']): component['node'] for component in
                                           existing_it_components}

            normalized_xml_it_components = {normalize_name(component['name']): component for component in
                                            components_data}
            missing_it_components = [component for norm_name, component in normalized_xml_it_components.items() if
                                     norm_name not in existing_it_component_names]

            logs['ITComponent'].extend(
                [{"Action": "Create", "FactSheet Type": "ITComponent", "Name": component['name']} for component in
                 missing_it_components])

            for component in missing_it_components:
                print(f"Test Mode: ITComponent factsheet {component['name']} created")

            # Technical Stack Functions
            def check_technical_stack_exists():
                technical_stack_query = """
                               {
                                 allFactSheets(factSheetType: TechnicalStack) {
                                   edges {
                                     node {
                                       id
                                       name
                                     }
                                   }
                                 }
                               }
                               """

                response6 = requests.post(url=request_url, headers=headers, json={"query": technical_stack_query})
                response6.raise_for_status()
                technical_stacks = {tech_stack['node']['name']: tech_stack['node']['id'] for tech_stack in
                                    response6.json()['data']['allFactSheets']['edges']}
                return technical_stacks

            existing_technical_stacks = check_technical_stack_exists()
            components_categories = set(component['category'] for component in components_data)
            missing_technical_stacks = [category for category in components_categories if
                                        category not in existing_technical_stacks]

            logs['TechnicalStack'].extend(
                [{"Action": "Create", "FactSheet Type": "TechnicalStack", "Name": stack} for stack in
                 missing_technical_stacks])

            for stack in missing_technical_stacks:
                print(f"Test Mode: TechnicalStack factsheet {stack} created")

            # Host
            components_response = requests.post(url=request_url, headers=headers, json={"query": component_query})
            components_response.raise_for_status()

            # Get existing IT Components
            existing_it_components = [component['node']['name'] for component in
                                      components_response.json()['data']['allFactSheets']['edges']]

            # Track existing IT component names in logs to ensure uniqueness
            existing_log_component_names = set(log['Name'] for log in logs['ITComponent'])

            for host in hosts_data:
                host_name = host['name']

                # Check if the IT component already exists in LeanIX or logs
                if host_name not in existing_it_components and host_name not in existing_log_component_names:
                    logs['ITComponent'].append({
                        "Action": "Create",
                        "FactSheet Type": "ITComponent",
                        "Name": host_name
                    })
                    print(f"Test Mode: ITComponent factsheet {host_name} created")
                    existing_log_component_names.add(host_name)

            # ============================================= Clients ================================================================
            def fetch_existing_applications(request_url, headers):
                graphql_query = """
                            {
                              allFactSheets(factSheetType: Application) {
                                edges {
                                  node {
                                    id
                                    name
                                    description
                                  }
                                }
                              }
                            }
                            """
                response = requests.post(url=request_url, headers=headers, json={"query": graphql_query})
                response.raise_for_status()
                applications = response.json()['data']['allFactSheets']['edges']
                return {normalize_name(app['node']['name']): app['node'] for app in applications}

            # Fetch existing applications
            existing_applications_dict = fetch_existing_applications(request_url, headers)

            for client in clients_data:
                second_level_app_name = f"{client['sid']}-{client['client_id']} {client['product_version']}"
                norm_second_level_app_name = normalize_name(second_level_app_name)

                # Check if the second-level application name already exists
                if norm_second_level_app_name not in existing_applications_dict:
                    print(f"Test Mode: Application factsheet {second_level_app_name} created")
                    logs['Application'].append({
                        "Action": "Create",
                        "FactSheet Type": "Application",
                        "Name": second_level_app_name
                    })

            # Fetch existing factsheets
            def query_factsheet():
                query = '''
                        {
                      allFactSheets(factSheetType: Application) {
                        edges {
                          node {
                          id
                          name
                        displayName
                          }
                        }
                      }
                    }
                        '''
                response16 = requests.post(request_url, headers=headers, json={'query': query})
                response16.raise_for_status()
                return response16.json()

            existing_factsheets_response = query_factsheet()

            existing_names = {node['node']['name'] for node in
                              existing_factsheets_response['data']['allFactSheets']['edges']}

            for system in modules_data:
                sub_module = system['acronym'] + "(" + system['sid'] + "-" + system['Client_id'] + ")"

                if sub_module not in existing_names:
                    print(f"Test Mode: Application factsheet {sub_module} created")
                    logs['Application'].append({
                        "Action": "Create",
                        "FactSheet Type": "Application",
                        "Name": sub_module
                    })

            # Process modules data to check for existing business capabilities
            def get_existing_business_capabilities():
                query = """
                        {
                          allFactSheets(factSheetType: BusinessCapability) {
                            edges {
                              node {
                                id
                                name
                              }
                            }
                          }
                        }
                        """
                response = requests.post(url=request_url, headers=headers, json={"query": query})
                response.raise_for_status()
                return {data['node']['name']: data['node']['id'] for data in
                        response.json()['data']['allFactSheets']['edges']}

            existing_business_capabilities = get_existing_business_capabilities()

            def process_modules_data(modules_data1, existing_business_capabilities1):
                for module in modules_data1:
                    module_name = module['name']

                    if module_name not in existing_business_capabilities1:
                        print(f"Test Mode: BusinessCapability factsheet {module_name} created")
                        logs['BusinessCapability'].append({
                            "Action": "Create",
                            "FactSheet Type": "BusinessCapability",
                            "Name": module_name
                        })

            process_modules_data(modules_data, existing_business_capabilities)

            # # ==================================================== Data Objects ======================

            # Load the Excel data
            file_path = 'data_object.xlsx'
            excel_data = pd.read_excel(file_path)

            # Extract the IDOC message types and their corresponding Langtext_EN
            idoc_data = excel_data[['IDOC_Message_Type', 'Langtext_EN']].to_dict(orient='records')

            # Fetch existing Data Objects
            def get_existing_data_objects():
                query = """
                        {
                          allFactSheets(factSheetType: DataObject) {
                            edges {
                              node {
                                id
                                name
                              }
                            }
                          }
                        }
                        """
                response = requests.post(url=request_url, headers=headers, json={"query": query})
                response.raise_for_status()
                return {data['node']['name']: data['node']['id'] for data in
                        response.json()['data']['allFactSheets']['edges']}

            existing_data_objects = get_existing_data_objects()

            idoc_dict = {item['IDOC_Message_Type']: item['Langtext_EN'] for item in idoc_data}

            # Check for existing Data Objects and log if they don't exist
            for ale_item in Ale_data:
                idoc_type = ale_item['idoc_messagetype']
                if idoc_type in idoc_dict:
                    langtext_en = idoc_dict[idoc_type]
                    if langtext_en not in existing_data_objects:
                        print(f"Test Mode: DataObject factsheet {langtext_en} created")
                        logs['DataObject'].append({
                            "Action": "Create",
                            "FactSheet Type": "DataObject",
                            "Name": langtext_en
                        })

            # # Fetch all existing interfaces
            def fetch_all_interfaces():
                query = """
                        query {
                          allFactSheets(factSheetType: Interface) {
                            edges {
                              node {
                                id
                                name
                              }
                            }
                          }
                        }
                        """
                response = requests.post(url=request_url, headers=headers, json={"query": query})
                response.raise_for_status()
                result = response.json()
                return {edge['node']['name'].lower(): edge['node']['id'] for edge in
                        result['data']['allFactSheets']['edges']}

            existing_interfaces = fetch_all_interfaces()

            # # Function to calculate similarity between two strings
            def similar(a, b):
                if a is None or b is None:
                    return 0
                return SequenceMatcher(None, a, b).ratio()

            # Match RFC data with Client data and Systems data
            def match_rfc_to_clients_and_systems(rfc_data, clients_data, systems_data):
                matched_data4 = []
                for rfc in rfc_data:
                    matched = False
                    for client in clients_data:
                        if similar(rfc['rfc_destination'], client['logical_system_name']) > 0.9:
                            sender = f"{rfc['sid']} {rfc['product_version']}"
                            receiver = f"{client['sid']}-{client['client_id']} {client['product_version']}"
                            if sender != receiver:
                                interface_name = f"{sender} ->> {receiver}"
                                matched_data4.append({
                                    'rfc_destination': rfc['rfc_destination'],
                                    'logical_system_name': client['logical_system_name'],
                                    'Interface_Name': interface_name,
                                    'Sender': sender,
                                    'Receiver': receiver

                                })
                            matched = True
                            break
                    if not matched:
                        for system in systems_data:
                            if similar(rfc['target'], system['host_ip']) > 0.9:
                                sender = f"{rfc['sid']} {rfc['product_version']}"
                                receiver = f"{system['sid']} {system['product_version']}"
                                if sender != receiver:
                                    interface_name = f"{sender} ->> {receiver}"
                                    matched_data4.append({
                                        'Interface_Name': interface_name,
                                        'Sender': sender,
                                        'Receiver': receiver,
                                        'target': rfc['target'],
                                        'host_ip': system['host_ip']
                                    })
                                matched = True
                                break
                    if not matched:
                        for system in systems_data:
                            if similar(rfc['target'], system['host_name']) > 0.9:
                                sender = f"{rfc['sid']} {rfc['product_version']}"
                                receiver = f"{system['sid']} {system['product_version']}"
                                if sender != receiver:
                                    interface_name = f"{sender} ->> {receiver}"
                                    matched_data4.append({
                                        'Interface_Name': interface_name,
                                        'Sender': sender,
                                        'Receiver': receiver,
                                        'target': rfc['target'],
                                        'host_name': system['host_name']
                                    })
                                break
                return matched_data4

            # Assuming Rfc_data, clients_data, systems_data are defined elsewhere in your script
            matched_data = match_rfc_to_clients_and_systems(Rfc_data, clients_data, systems_data)
            for match in matched_data:
                interface_name = match['Interface_Name'].lower()
                if interface_name not in existing_interfaces:
                    print(f"Test Mode: Interface factsheet {interface_name} created 891")

            # Function to add matched interfaces to logs if they do not exist
            def add_new_interfaces_to_logs(matched_data, existing_interfaces, logs):
                for match in matched_data:
                    interface_name = match['Interface_Name'].lower()
                    if interface_name not in existing_interfaces:
                        logs['Interface'].append({
                            "Action": "Create",
                            "FactSheet Type": "Interface",
                            "Name": interface_name
                        })

            # Add new interfaces to logs
            add_new_interfaces_to_logs(matched_data, existing_interfaces, logs)

            def create_interfaces(clients_data2, ale_data2):
                interfaces5 = []
                for ale in ale_data2:
                    sender_matches = [client for client in clients_data2 if
                                      client['logical_system_name'] == ale['sender']]
                    receiver_matches = [client for client in clients_data2 if
                                        client['logical_system_name'] == ale['receiver']]
                    for sender in sender_matches:
                        for receiver in receiver_matches:
                            interface_name = f"{sender['sid']}-{sender['client_id']} {sender['product_version']} ->> {receiver['sid']}-{receiver['client_id']} {receiver['product_version']}"
                            interfaces5.append({
                                "interface_name": interface_name,
                                "idoc_messagetype": ale['idoc_messagetype']
                            })
                return interfaces5

            # Assuming clients_data and Ale_data are defined elsewhere in your script
            interfaces = create_interfaces(clients_data, Ale_data)

            # Convert interfaces to DataFrame and drop duplicates
            matched_df2 = pd.DataFrame(interfaces)
            matched_df2 = matched_df2.drop_duplicates()

            # Function to add matched interfaces to logs if they do not exist
            def add_new_interfaces_to_logs2(matched_df, existing_interfaces, logs):
                for _, row in matched_df.iterrows():
                    interface_name = row['interface_name'].lower()
                    if interface_name not in existing_interfaces:
                        logs['Interface'].append({
                            "Action": "Create",
                            "FactSheet Type": "Interface",
                            "Name": interface_name

                        })

            # Add new interfaces to logs
            add_new_interfaces_to_logs2(matched_df2, existing_interfaces, logs)
            for _, row in matched_df2.iterrows():
                interface_name = row['interface_name'].lower()
                if interface_name not in existing_interfaces:
                    print(f"Test Mode: Interface factsheet {interface_name} created 947")

        file_path2 = save_factsheet_logs_to_excel(logs, "test_mode_data", test_mode=True)
        return file_path2

    if test_mode and delta_operation:
        print("Test Mode is ON. No actual import will be performed.")
        for run in range(3):
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
            xml_applications = {f"{system['sid']} {system['product_version']}": system['description'] for system in
                                systems_data}
            normalized_xml_applications = {normalize_name(app): app for app in xml_applications}
            applications_to_create = {app: xml_applications[app] for norm_app, app in
                                      normalized_xml_applications.items() if
                                      norm_app not in existing_app_names}
            created_applications = [f"{system['sid']} {system['product_version']}" for system in systems_data if
                                    f"{system['sid']} {system['product_version']}" in applications_to_create]
            logs['Application'].extend(
                [{"Action": "Create", "FactSheet Type": "Application", "Name": app} for app in created_applications])

            for app in created_applications:
                print(f"Test Mode: Application factsheet {app} created")

            # Update existing applications
            for norm_app_name, app_name in normalized_xml_applications.items():
                if norm_app_name in existing_app_names:
                    existing_app = existing_app_names[norm_app_name]
                    application_id = existing_app['id']
                    current_description = existing_app['description']
                    new_description = xml_applications[app_name]

                    if current_description != new_description:
                        logs['Application'].append({
                            "Action": "Update",
                            "FactSheet Type": "Application",
                            "Name": app_name,
                            "Description": new_description
                        })
                        print(f"Test Mode: Application factsheet {app_name} updated")

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
            existing_it_components = response3.json()['data']['allFactSheets']['edges']
            existing_it_component_names = {normalize_name(component['node']['name']): component['node'] for component in
                                           existing_it_components}

            normalized_xml_it_components = {normalize_name(component['name']): component for component in
                                            components_data}
            missing_it_components = [component for norm_name, component in normalized_xml_it_components.items() if
                                     norm_name not in existing_it_component_names]

            logs['ITComponent'].extend(
                [{"Action": "Create", "FactSheet Type": "ITComponent", "Name": component['name']} for component in
                 missing_it_components])
            for component in missing_it_components:
                print(f"Test Mode: ITComponent factsheet {component['name']} created")

            # Function to normalize and compare descriptions
            def normalize_description(description):
                return description.strip() if description else ''

            # Update existing IT components
            for norm_component_name, component in normalized_xml_it_components.items():
                if norm_component_name in existing_it_component_names:
                    existing_component = existing_it_component_names[norm_component_name]
                    component_id = existing_component['id']
                    current_description = normalize_description(existing_component['description'])
                    new_description = normalize_description(component['description'])

                    if current_description != new_description:
                        logs['ITComponent'].append({
                            "Action": "Update",
                            "FactSheet Type": "ITComponent",
                            "Name": component['name'],
                            "Description": new_description
                        })
                        for component in missing_it_components:
                            print(f"Test Mode: ITComponent factsheet {component['name']} updated")

            # Technical Stack Functions
            def check_technical_stack_exists():
                technical_stack_query = """
                                               {
                                                 allFactSheets(factSheetType: TechnicalStack) {
                                                   edges {
                                                     node {
                                                       id
                                                       name
                                                     }
                                                   }
                                                 }
                                               }
                                               """

                response6 = requests.post(url=request_url, headers=headers, json={"query": technical_stack_query})
                response6.raise_for_status()
                technical_stacks = {tech_stack['node']['name']: tech_stack['node']['id'] for tech_stack in
                                    response6.json()['data']['allFactSheets']['edges']}
                return technical_stacks

            existing_technical_stacks = check_technical_stack_exists()
            components_categories = set(component['category'] for component in components_data)
            missing_technical_stacks = [category for category in components_categories if
                                        category not in existing_technical_stacks]

            logs['TechnicalStack'].extend(
                [{"Action": "Create", "FactSheet Type": "TechnicalStack", "Name": stack} for stack in
                 missing_technical_stacks])

            for stack in missing_technical_stacks:
                print(f"Test Mode: TechnicalStack factsheet {stack} created")

            # Host-----------------------------------------------------------------------------------------------------
            components_response = requests.post(url=request_url, headers=headers, json={"query": component_query})
            components_response.raise_for_status()

            # Get existing IT Components
            existing_it_components = [component['node']['name'] for component in
                                      components_response.json()['data']['allFactSheets']['edges']]

            # Track existing IT component names in logs to ensure uniqueness
            existing_log_component_names = set(log['Name'] for log in logs['ITComponent'])

            for host in hosts_data:
                host_name = host['name']

                # Check if the IT component already exists in LeanIX or logs
                if host_name not in existing_it_components and host_name not in existing_log_component_names:
                    logs['ITComponent'].append({
                        "Action": "Create",
                        "FactSheet Type": "ITComponent",
                        "Name": host_name
                    })
                    print(f"Test Mode: ITComponent factsheet {host_name} created")
                    existing_log_component_names.add(host_name)

                # Function to update IT Component alias
                def update_it_component_alias(component_id, alias, category):
                    mutation = """
                           mutation ($id: ID!, $patches: [Patch]!) {
                               updateFactSheet(id: $id, patches: $patches) {
                                   factSheet {
                                       id
                                       name
                                       ... on ITComponent {
                                           alias
                                       }
                                   }
                               }
                           }
                           """
                    variables = {
                        "id": component_id,
                        "patches": [
                            {
                                "op": "replace",
                                "path": "/alias",
                                "value": alias
                            },
                            {
                                "op": "replace",
                                "path": "/description",
                                "value": category
                            }
                        ]
                    }
                    data = {"query": mutation, "variables": variables}
                    response = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                    response.raise_for_status()

                component_query = """
                           {
                             allFactSheets(factSheetType: ITComponent) {
                               edges {
                                 node {
                                   id
                                   name
                                   description


                                   category
                                   ... on ITComponent {
                                     alias
                                     tags {
                                       name
                                     }
                                   }
                                 }
                               }
                             }
                           }
                           """
                response3 = requests.post(url=request_url, headers=headers, json={"query": component_query})
                response3.raise_for_status()

                # Check if the response contains errors
                response_data = response3.json()
                if 'errors' in response_data:
                    print("Error in response:", response_data['errors'])
                    leanix_it_components = []
                else:
                    leanix_it_components_data = response_data.get('data', {}).get('allFactSheets', {}).get('edges', [])
                    if leanix_it_components_data is None:
                        leanix_it_components_data = []
                    leanix_it_components = [node['node'] for node in leanix_it_components_data]

                # Prepare a dictionary of existing IT components for quick lookup
                existing_it_components_dict = {comp['name']: comp for comp in leanix_it_components}

                # Compare and update IT components if there are changes in alias (IP address) or category (description)

                for host in hosts_data:
                    host_name = host['name']
                    ip_address = host['ip_address']
                    category = host['category']

                    if host_name in existing_it_components_dict:
                        existing_component = existing_it_components_dict[host_name]
                        existing_alias = existing_component.get('alias', '')
                        existing_description = existing_component.get('description', '')

                        # Check if alias (IP address) or description (category) has changed
                        if existing_alias != ip_address or existing_description != category:
                            update_it_component_alias(existing_component['id'], ip_address, category)
                            logs['ITComponent'].append({
                                "Action": "Update",
                                "FactSheet Type": "ITComponent",
                                "Name": host_name,
                                "Alias": ip_address
                            })
                            print(f"Test Mode: ITComponent factsheet {host_name} updated")

                    # else:
                    #     print(f"IT Component {host_name} does not exist in LeanIX")
                    #     logs['ITComponent'].append({
                    #         "Action": "Create",
                    #         "FactSheet Type": "ITComponent",
                    #         "Name": host_name,
                    #         "Alias": ip_address,
                    #         "Category": category,
                    #         "Description": "New IT Component created"
                    #     })

            # ============================================= Clients ================================================================
            def fetch_existing_applications(request_url, headers):
                graphql_query = """
                                        {
                                          allFactSheets(factSheetType: Application) {
                                            edges {
                                              node {
                                                id
                                                name
                                                description
                                              }
                                            }
                                          }
                                        }
                                        """
                response = requests.post(url=request_url, headers=headers, json={"query": graphql_query})
                response.raise_for_status()
                applications = response.json()['data']['allFactSheets']['edges']
                return {normalize_name(app['node']['name']): app['node'] for app in applications}

            # Fetch existing applications
            existing_applications_dict = fetch_existing_applications(request_url, headers)

            for client in clients_data:
                second_level_app_name = f"{client['sid']}-{client['client_id']} {client['product_version']}"
                norm_second_level_app_name = normalize_name(second_level_app_name)

                # Check if the second-level application name already exists
                if norm_second_level_app_name not in existing_applications_dict:
                    logs['Application'].append({
                        "Action": "Create",
                        "FactSheet Type": "Application",
                        "Name": second_level_app_name
                    })
                    print(f"Test Mode: Application factsheet {second_level_app_name} created")

            # Function to update application description
            def update_application_description(application_id, description):
                mutation = """
                        mutation ($id: ID!, $patches: [Patch]!) {
                            updateFactSheet(id: $id, patches: $patches) {
                                factSheet {
                                    id
                                    name
                                    description
                                }
                            }
                        }
                        """
                variables = {
                    "id": application_id,
                    "patches": [{"op": "replace", "path": "/description", "value": description}]
                }
                data = {"query": mutation, "variables": variables}
                response = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                response.raise_for_status()

            # Process clients data
            for client in clients_data:
                it_component_name = f"{client['sid']}-{client['client_id']} {client['product_version']}"
                new_description = client['description']
                norm_name = normalize_name(it_component_name)

                if norm_name in existing_applications_dict:
                    app_id = existing_applications_dict[norm_name]['id']
                    current_description = existing_applications_dict[norm_name]['description']

                    if new_description != current_description:
                        update_application_description(app_id, new_description)
                        logs['Application'].append({
                            "Action": "Update",
                            "FactSheet Type": "Application",
                            "Name": it_component_name,
                            "Description": new_description
                        })
                        print(f"Test Mode: Application factsheet {norm_name} updated")

            # Module factsheet --------------------------------------------------------------------------------------------
            def query_factsheet():
                query = '''
                                    {
                                  allFactSheets(factSheetType: Application) {
                                    edges {
                                      node {
                                      id
                                      name
                                    displayName
                                      }
                                    }
                                  }
                                }
                                    '''
                response16 = requests.post(request_url, headers=headers, json={'query': query})
                response16.raise_for_status()
                return response16.json()

            existing_factsheets_response = query_factsheet()

            existing_names = {node['node']['name'] for node in
                              existing_factsheets_response['data']['allFactSheets']['edges']}

            for system in modules_data:
                sub_module = system['acronym'] + "(" + system['sid'] + "-" + system['Client_id'] + ")"

                if sub_module not in existing_names:
                    print(f"Test Mode: Application factsheet {sub_module} created")
                    logs['Application'].append({
                        "Action": "Create",
                        "FactSheet Type": "Application",
                        "Name": sub_module
                    })

            # Process modules data to check for existing business capabilities
            def get_existing_business_capabilities():
                query = """
                                    {
                                      allFactSheets(factSheetType: BusinessCapability) {
                                        edges {
                                          node {
                                            id
                                            name
                                          }
                                        }
                                      }
                                    }
                                    """
                response = requests.post(url=request_url, headers=headers, json={"query": query})
                response.raise_for_status()
                return {data['node']['name']: data['node']['id'] for data in
                        response.json()['data']['allFactSheets']['edges']}

            existing_business_capabilities = get_existing_business_capabilities()

            def process_modules_data(modules_data1, existing_business_capabilities1):
                for module in modules_data1:
                    module_name = module['name']
                    if module_name not in existing_business_capabilities1:
                        logs['BusinessCapability'].append({
                            "Action": "Create",
                            "FactSheet Type": "BusinessCapability",
                            "Name": module_name
                        })
                        print(f"Test Mode: BusinessCapability factsheet {module_name} created")

            process_modules_data(modules_data, existing_business_capabilities)

            # # ==================================================== Data Objects ======================

            # Load the Excel data
            file_path = 'data_object.xlsx'
            excel_data = pd.read_excel(file_path)

            # Extract the IDOC message types and their corresponding Langtext_EN
            idoc_data = excel_data[['IDOC_Message_Type', 'Langtext_EN']].to_dict(orient='records')

            # Fetch existing Data Objects
            def get_existing_data_objects():
                query = """
                                    {
                                      allFactSheets(factSheetType: DataObject) {
                                        edges {
                                          node {
                                            id
                                            name
                                          }
                                        }
                                      }
                                    }
                                    """
                response = requests.post(url=request_url, headers=headers, json={"query": query})
                response.raise_for_status()
                return {data['node']['name']: data['node']['id'] for data in
                        response.json()['data']['allFactSheets']['edges']}

            existing_data_objects = get_existing_data_objects()

            idoc_dict = {item['IDOC_Message_Type']: item['Langtext_EN'] for item in idoc_data}

            # Check for existing Data Objects and log if they don't exist
            for ale_item in Ale_data:
                idoc_type = ale_item['idoc_messagetype']
                if idoc_type in idoc_dict:
                    langtext_en = idoc_dict[idoc_type]
                    if langtext_en not in existing_data_objects:
                        logs['DataObject'].append({
                            "Action": "Create",
                            "FactSheet Type": "DataObject",
                            "Name": langtext_en
                        })
                        print(f"Test Mode: DataObject factsheet {langtext_en} created")

            # # Fetch all existing interfaces
            def fetch_all_interfaces():
                query = """
                                    query {
                                      allFactSheets(factSheetType: Interface) {
                                        edges {
                                          node {
                                            id
                                            name
                                          }
                                        }
                                      }
                                    }
                                    """
                response = requests.post(url=request_url, headers=headers, json={"query": query})
                response.raise_for_status()
                result = response.json()
                return {edge['node']['name'].lower(): edge['node']['id'] for edge in
                        result['data']['allFactSheets']['edges']}

            existing_interfaces = fetch_all_interfaces()

            # # Function to calculate similarity between two strings
            def similar(a, b):
                if a is None or b is None:
                    return 0
                return SequenceMatcher(None, a, b).ratio()

            # Match RFC data with Client data and Systems data
            def match_rfc_to_clients_and_systems(rfc_data, clients_data, systems_data):
                matched_data4 = []
                for rfc in rfc_data:
                    matched = False
                    for client in clients_data:
                        if similar(rfc['rfc_destination'], client['logical_system_name']) > 0.9:
                            sender = f"{rfc['sid']} {rfc['product_version']}"
                            receiver = f"{client['sid']}-{client['client_id']} {client['product_version']}"
                            if sender != receiver:
                                interface_name = f"{sender} ->> {receiver}"
                                matched_data4.append({
                                    'rfc_destination': rfc['rfc_destination'],
                                    'logical_system_name': client['logical_system_name'],
                                    'Interface_Name': interface_name,
                                    'Sender': sender,
                                    'Receiver': receiver

                                })
                            matched = True
                            break
                    if not matched:
                        for system in systems_data:
                            if similar(rfc['target'], system['host_ip']) > 0.9:
                                sender = f"{rfc['sid']} {rfc['product_version']}"
                                receiver = f"{system['sid']} {system['product_version']}"
                                if sender != receiver:
                                    interface_name = f"{sender} ->> {receiver}"
                                    matched_data4.append({
                                        'Interface_Name': interface_name,
                                        'Sender': sender,
                                        'Receiver': receiver,
                                        'target': rfc['target'],
                                        'host_ip': system['host_ip']
                                    })
                                matched = True
                                break
                    if not matched:
                        for system in systems_data:
                            if similar(rfc['target'], system['host_name']) > 0.9:
                                sender = f"{rfc['sid']} {rfc['product_version']}"
                                receiver = f"{system['sid']} {system['product_version']}"
                                if sender != receiver:
                                    interface_name = f"{sender} ->> {receiver}"
                                    matched_data4.append({
                                        'Interface_Name': interface_name,
                                        'Sender': sender,
                                        'Receiver': receiver,
                                        'target': rfc['target'],
                                        'host_name': system['host_name']
                                    })
                                break
                return matched_data4

            # Assuming Rfc_data, clients_data, systems_data are defined elsewhere in your script
            matched_data = match_rfc_to_clients_and_systems(Rfc_data, clients_data, systems_data)

            # Function to add matched interfaces to logs if they do not exist
            def add_new_interfaces_to_logs(matched_data, existing_interfaces, logs):
                for match in matched_data:
                    interface_name = match['Interface_Name'].lower()
                    if interface_name not in existing_interfaces:
                        logs['Interface'].append({
                            "Action": "Create",
                            "FactSheet Type": "Interface",
                            "Name": interface_name
                        })

            # Add new interfaces to logs
            add_new_interfaces_to_logs(matched_data, existing_interfaces, logs)

            def create_interfaces(clients_data2, ale_data2):
                interfaces5 = []
                for ale in ale_data2:
                    sender_matches = [client for client in clients_data2 if
                                      client['logical_system_name'] == ale['sender']]
                    receiver_matches = [client for client in clients_data2 if
                                        client['logical_system_name'] == ale['receiver']]
                    for sender in sender_matches:
                        for receiver in receiver_matches:
                            interface_name = f"{sender['sid']}-{sender['client_id']} {sender['product_version']} ->> {receiver['sid']}-{receiver['client_id']} {receiver['product_version']}"
                            interfaces5.append({
                                "interface_name": interface_name,
                                "idoc_messagetype": ale['idoc_messagetype']
                            })
                return interfaces5

            # Assuming clients_data and Ale_data are defined elsewhere in your script
            interfaces = create_interfaces(clients_data, Ale_data)

            # Convert interfaces to DataFrame and drop duplicates
            matched_df2 = pd.DataFrame(interfaces)
            matched_df2 = matched_df2.drop_duplicates()

            # Function to add matched interfaces to logs if they do not exist
            def add_new_interfaces_to_logs2(matched_df, existing_interfaces, logs):
                for _, row in matched_df.iterrows():
                    interface_name = row['interface_name'].lower()
                    if interface_name not in existing_interfaces:
                        logs['Interface'].append({
                            "Action": "Create",
                            "FactSheet Type": "Interface",
                            "Name": interface_name

                        })
                        print(f"Test Mode: Interface factsheet {interface_name} created")

            # Add new interfaces to logs
            add_new_interfaces_to_logs2(matched_df2, existing_interfaces, logs)

            # Save logs to Excel
        file_path1 = save_factsheet_logs_to_excel(logs, "test_mode_data", test_mode=True)
        return file_path1

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
                        log_and_save('Application', {'Action': 'Cancelled', 'Message': 'User canceled operation'})
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
                    log_and_save('Application', {
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
                    log_and_save('Application', {'Action': 'Cancelled', 'Message': 'User canceled operation'})
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

                # Compare and Identify Unique Missing IT Components
                # Using a set to ensure unique components based on their 'name'
                unique_missing_it_components = list({component['name']: component for component in components_data
                                                     if component['name'] not in leanix_it_components}.values())

                # Create missing IT Components
                for component in unique_missing_it_components:
                    # Check if user canceled during the loop
                    if user_cancel:
                        log_and_save('ITComponent', {'Action': 'Cancelled', 'Message': 'User canceled operation'})
                        return excel_file_path

                    name1 = component['name']
                    description1 = component.get('description', '')  # Handle missing descriptions safely
                    release1 = component.get('release', '')  # Handle missing releases safely

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
                        # Log the error and skip to the next component
                        log_and_save('ITComponent',
                                     {'Action': 'Error', 'Name': name1, 'Message': response_data_create['errors']})
                        continue  # Skip to the next component

                    # Extract the ID if present
                    try:
                        it_component_id = response_data_create['data']['createFactSheet']['factSheet']['id']

                        # Log the creation action with ID
                        log_and_save('ITComponent', {
                            'Name': name1,
                            'Action': 'Created',
                            'FactSheet Type': 'ITComponent',
                            'ID': it_component_id  # Include the ID in the log
                        })

                        print(f"IT Component factsheet {name1} created")
                    except KeyError:
                        print(f"Unexpected response structure. Could not find the 'id' for IT Component {name1}.")
                        log_and_save('ITComponent', {
                            'Name': name1,
                            'Action': 'Error',
                            'Message': 'Unexpected response structure - missing ID.'
                        })

                # Save logs after processing IT components
                save_factsheet_logs_to_excel(logs)

                # Check if user canceled after processing IT components
                if user_cancel:
                    log_and_save('ITComponent', {'Action': 'Cancelled', 'Message': 'User canceled operation'})
                    return excel_file_path

                # Retrieve all applications and IT components again after creation
                applications_response = requests.post(url=request_url, headers=headers,
                                                      json={"query": application_query})
                applications_response.raise_for_status()
                applications = {app['node']['displayName']: app['node']['id'] for app in
                                applications_response.json()['data']['allFactSheets']['edges']}

                components_response = requests.post(url=request_url, headers=headers,
                                                    json={"query": component_query})
                components_response.raise_for_status()
                components = {component['node']['name']: component['node']['id'] for component in
                              components_response.json()['data']['allFactSheets']['edges']}

                # Create relationships for software components from Components.xml
                existing_relationships_query_components = """
                              {
                                allFactSheets(factSheetType: Application) {
                                  edges {
                                    node {
                                      id
                                      name
                                      ... on Application {
                                        relApplicationToITComponent {
                                          edges {
                                            node {
                                              id
                                              type
                                              factSheet {
                                                id
                                                name
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                              """

                response12 = requests.post(url=request_url, headers=headers,
                                           json={"query": existing_relationships_query_components})
                response12.raise_for_status()
                existing_relationships_components = response12.json()['data']['allFactSheets']['edges']

                existing_relationships_components_set = set()
                for app in existing_relationships_components:
                    app_id = app['node']['id']
                    for relationship in app['node']['relApplicationToITComponent']['edges']:
                        component_id = relationship['node']['factSheet']['id']
                        existing_relationships_components_set.add((app_id, component_id))

                relationship_data_components = []

                for component in components_data:
                    app_name = f"{component['sid']} {component['product_version']}"
                    component_name = component['name']

                    if app_name in applications and component_name in components:
                        app_id = applications[app_name]
                        component_id = components[component_name]

                        if (app_id, component_id) not in existing_relationships_components_set:
                            relationship_data_components.append({
                                "Application ID": app_id,
                                "Application Name": app_name,
                                "IT Component ID": component_id,
                                "IT Component Name": component_name
                            })

                relationship_df_components = pd.DataFrame(relationship_data_components)
                relationship_df_components = relationship_df_components.drop_duplicates()

                for _, relationship in relationship_df_components.iterrows():
                    app_id = relationship["Application ID"]
                    component_id = relationship["IT Component ID"]

                    relationship_mutation = """
                                  mutation {
                                      updateFactSheet(id: "%s", patches: [{op: add, path: "/relApplicationToITComponent/new_1", value: "%s"}]) {
                                          factSheet {
                                              id
                                              name
                                              ... on Application {
                                                  relApplicationToITComponent {
                                                      edges {
                                                          node {
                                                              id
                                                          }
                                                      }
                                                  }
                                              }
                                          }
                                      }
                                  }
                                  """ % (app_id, component_id)

                    relation_response = requests.post(url=request_url, headers=headers,
                                                      data=json.dumps({"query": relationship_mutation}))
                    relation_response.raise_for_status()

                    print(
                        f"Created relationship from Application '{relationship['Application Name']}' to IT "
                        f"Component '{relationship['IT Component Name']}'")

                # ------------------------------------------ Technical Stack --------------------------------------------------
                # Technical Stack Functions
                def check_technical_stack_exists():
                    technical_stack_query = """
                                           {
                                             allFactSheets(factSheetType: TechnicalStack) {
                                               edges {
                                                 node {
                                                   id
                                                   name
                                                 }
                                               }
                                             }
                                           }
                                           """

                    response6 = requests.post(url=request_url, headers=headers,
                                              json={"query": technical_stack_query})
                    response6.raise_for_status()
                    technical_stacks = {tech_stack['node']['name']: tech_stack['node']['id'] for tech_stack in
                                        response6.json()['data']['allFactSheets']['edges']}
                    return technical_stacks

                def create_technical_stack(tech_stack_name2):
                    mutation1 = """
                                               mutation ($input: BaseFactSheetInput!, $patches: [Patch]!) {
                                                   createFactSheet(input: $input, patches: $patches) {
                                                       factSheet {
                                                           id
                                                           name
                                                           type
                                                           tags {
                                                               id
                                                               name
                                                           }
                                                       }
                                                   }
                                               }
                                               """
                    variables1 = {
                        "input": {
                            "name": tech_stack_name2,
                            "type": "TechnicalStack"
                        },
                        "patches": [
                            {
                                "op": "add",
                                "path": "/tags",
                                "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'
                            }
                        ]
                    }

                    data1 = {"query": mutation1, "variables": variables1}

                    response7 = requests.post(url=request_url, headers=headers, data=json.dumps(data1))
                    response7.raise_for_status()
                    print(f" Technical Stack {tech_stack_name2} Created")
                    tss = response7.json()
                    ts_id = tss['data']['createFactSheet']['factSheet']['id']

                    # Log the creation action
                    logs['TechnicalStack'].append({
                        'Name': tech_stack_name2,
                        'Action': 'Created',
                        'FactSheet Type': 'TechnicalStack',
                        'ID': ts_id
                    })

                existing_technical_stacks = check_technical_stack_exists()
                components_categories = set(component['category'] for component in components_data)
                missing_technical_stacks = [category for category in components_categories if
                                            category not in existing_technical_stacks]

                for tech_stack_name in missing_technical_stacks:
                    if user_cancel:
                        log_and_save('TechnicalStack',
                                     {'Action': 'Cancelled', 'Message': 'User canceled operation'})
                        return excel_file_path
                    create_technical_stack(tech_stack_name)

                # Fetch the updated list of technical stacks and IT components after creation
                technical_stacks = check_technical_stack_exists()
                response9 = requests.post(url=request_url, headers=headers, json={"query": component_query})
                response9.raise_for_status()
                it_components = {it_component['node']['name']: it_component['node']['id'] for it_component in
                                 response9.json()['data']['allFactSheets']['edges']}

                # Retrieve existing relationships
                existing_relationships_query = """
                                           {
                                             allFactSheets(factSheetType: ITComponent) {
                                               edges {
                                                 node {
                                                   id
                                                   name
                                                   ... on ITComponent {
                                                     relITComponentToTechnologyStack {
                                                       edges {
                                                         node {
                                                           id
                                                           type
                                                           factSheet {
                                                             id
                                                             name
                                                           }
                                                         }
                                                       }
                                                     }
                                                   }
                                                 }
                                               }
                                             }
                                           }
                                           """

                response10 = requests.post(url=request_url, headers=headers,
                                           json={"query": existing_relationships_query})
                response10.raise_for_status()
                existing_relationships = response10.json()['data']['allFactSheets']['edges']

                existing_relationships_set = set()
                for it_component in existing_relationships:
                    component_id = it_component['node']['id']
                    for relationship in it_component['node']['relITComponentToTechnologyStack']['edges']:
                        technical_stack_id = relationship['node']['factSheet']['id']
                        existing_relationships_set.add((component_id, technical_stack_id))

                relationship_data = []

                # Prepare unique relationship data before making the API calls
                for component in components_data:
                    component_name = component['name']
                    component_category = component['category']

                    if component_name in it_components and component_category in technical_stacks:
                        component_id = it_components[component_name]
                        technical_stack_id = technical_stacks[component_category]

                        if (component_id, technical_stack_id) not in existing_relationships_set:
                            relationship_data.append({
                                "IT Component ID": component_id,
                                "IT Component Name": component_name,
                                "Technical Stack ID": technical_stack_id,
                                "Technical Stack Name": component_category
                            })

                # Convert relationship data to a DataFrame
                relationship_df = pd.DataFrame(relationship_data)

                # Drop duplicates to ensure unique relationships
                relationship_df = relationship_df.drop_duplicates()

                # Proceed with the API calls to create the unique relationships
                for _, relationship in relationship_df.iterrows():
                    component_id = relationship["IT Component ID"]
                    technical_stack_id = relationship["Technical Stack ID"]

                    relationship_mutation = """
                                                mutation {
                                           updateFactSheet(
                                             id: "%s"
                                             patches: [{op: add, path: "/relITComponentToTechnologyStack/new_1", value: "%s"}]
                                           ) {
                                             factSheet {
                                               id
                                               name
                                               ... on ITComponent {
                                                 relITComponentToTechnologyStack {
                                                   edges {
                                                     node {
                                                       id
                                                     }
                                                   }
                                                 }
                                               }
                                             }
                                           }
                                           }
                                               """ % (component_id, technical_stack_id)

                    relation_response = requests.post(url=request_url, headers=headers,
                                                      data=json.dumps({"query": relationship_mutation}))
                    relation_response.raise_for_status()

                    print(
                        f"Created relationship between IT Component '{relationship['IT Component Name']}' and Technical Stack '{relationship['Technical Stack Name']}'")

                # ============================================= Hosts ================================================================

                # Get existing IT Components
                existing_it_components = [component['node']['name'] for component in
                                          components_response.json()['data']['allFactSheets']['edges']]

                # A set to track the names of IT Components that have been created
                created_it_components = set()

                for host in hosts_data:

                    host_name = host['name']
                    ip_address = host['ip_address']

                    # Check if user canceled
                    if user_cancel:
                        log_and_save('ITComponent', {'Action': 'Cancelled', 'Message': 'User canceled operation'})
                        return excel_file_path

                    # Check if the IT component already exists before creating
                    if host_name in existing_it_components or host_name in created_it_components:
                        continue  # Skip this host as it already exists or was just created

                    # Proceed with creation if it doesn't exist
                    mutation = """
                        mutation ($input: BaseFactSheetInput!, $patches: [Patch]!) {
                            createFactSheet(input: $input, patches: $patches) {
                                factSheet {
                                    id
                                    name
                                    type
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
                            "name": host_name,
                            "type": "ITComponent"
                        },
                        "patches": [
                            {
                                "op": "add",
                                "path": "/category",
                                "value": "hardware"
                            },
                            {
                                "op": "add",
                                "path": "/tags",
                                "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'
                            },
                            {
                                "op": "add",
                                "path": "/alias",
                                "value": ip_address
                            }
                        ]
                    }

                    data = {"query": mutation, "variables": variables}

                    response10 = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                    it2 = response10.json()

                    # Check if the response contains the expected data and not just an error
                    if it2 and 'data' in it2 and 'createFactSheet' in it2['data'] and 'factSheet' in it2['data'][
                        'createFactSheet']:
                        it_2 = it2['data']['createFactSheet']['factSheet']['id']
                        # Log the creation action
                        logs['ITComponent'].append({
                            'Name': host_name,
                            'Action': 'Created',
                            'FactSheet Type': 'ITComponent',
                            'ID': it_2
                        })
                        # Add this IT Component name to the created set to avoid future duplicates
                        created_it_components.add(host_name)
                        print(f"IT Component factsheet {host_name} Created")
                    else:
                        # Handle the error case
                        if 'errors' in it2:
                            print(f"Error creating IT Component for host {host_name}: {it2['errors']}")
                        else:
                            print(
                                f"Unexpected response structure while creating IT Component for host {host_name}: {it2}")
                        continue  # Move on to the next host

                # Fetch existing relationships for hosts
                existing_relationships_query_hosts = """
                                       {
                                         allFactSheets(factSheetType: Application) {
                                           edges {
                                             node {
                                               id
                                               name
                                               ... on Application {
                                                 relApplicationToITComponent {
                                                   edges {
                                                     node {
                                                       id
                                                       type
                                                       factSheet {
                                                         id
                                                         name
                                                       }
                                                     }
                                                   }
                                                 }
                                               }
                                             }
                                           }
                                         }
                                       }
                                       """

                response11 = requests.post(url=request_url, headers=headers,
                                           json={"query": existing_relationships_query_hosts})
                response11.raise_for_status()
                existing_relationships_hosts = response11.json()['data']['allFactSheets']['edges']

                existing_relationships_hosts_set = set()
                for app in existing_relationships_hosts:
                    app_id = app['node']['id']
                    for relationship in app['node']['relApplicationToITComponent']['edges']:
                        component_id = relationship['node']['factSheet']['id']
                        existing_relationships_hosts_set.add((app_id, component_id))

                    # Prepare unique relationship data before making the API calls
                relationship_data_hosts = []

                for host in hosts_data:
                    app_name = f"{host['sid']} {host['product_version']}"
                    host_name = host['name']

                    if app_name in applications and host_name in it_components:
                        app_id = applications[app_name]
                        component_id = it_components[host_name]

                        if (app_id, component_id) not in existing_relationships_hosts_set:
                            relationship_data_hosts.append({
                                "Application ID": app_id,
                                "Application Name": app_name,
                                "IT Component ID": component_id,
                                "IT Component Name": host_name
                            })

                # Convert relationship data to a DataFrame
                relationship_df_hosts = pd.DataFrame(relationship_data_hosts)

                # Drop duplicates to ensure unique relationships
                relationship_df_hosts = relationship_df_hosts.drop_duplicates()

                # Proceed with the API calls to create the unique relationships
                for _, relationship in relationship_df_hosts.iterrows():
                    app_id = relationship["Application ID"]
                    component_id = relationship["IT Component ID"]

                    relationship_mutation = """
                                              mutation {
                                                  updateFactSheet(id: "%s", patches: [{op: add, path: "/relApplicationToITComponent/new_1", value: "%s"}]) {
                                                      factSheet {
                                                          id
                                                          name
                                                          ... on Application {
                                                              relApplicationToITComponent {
                                                                  edges {
                                                                      node {
                                                                          id
                                                                      }
                                                                  }
                                                              }
                                                          }
                                                      }
                                                  }
                                              }
                                              """ % (app_id, component_id)

                    relation_response = requests.post(url=request_url, headers=headers,
                                                      data=json.dumps({"query": relationship_mutation}))
                    relation_response.raise_for_status()

                    print(
                        f"Created relationship from Application '{relationship['Application Name']}' to IT Component '{relationship['IT Component Name']}'")

                # ============================================= Clients ================================================================

                # Fetch existing IT Components
                existing_it_components = {component['node']['name']: component['node']['id'] for component in
                                          components_response.json()['data']['allFactSheets']['edges']}

                def fetch_existing_it_components(request_url, headers):
                    graphql_query = """
                                           {
                                             allFactSheets(factSheetType: Application) {
                                               edges {
                                                 node {
                                                   id
                                                   name
                                                 }
                                               }
                                             }
                                           }
                                           """
                    response124 = requests.post(url=request_url, headers=headers, json={"query": graphql_query})
                    response124.raise_for_status()
                    it_components4 = {node['node']['name']: node['node']['id'] for node in
                                      response124.json()['data']['allFactSheets']['edges']}
                    return it_components4

                def create_client_application(clients_data, existing_it_components, request_url, headers):
                    for client in clients_data:
                        it_component_name = client['sid'] + "-" + client['client_id'] + ' ' + client[
                            'product_version']
                        description = client['description']
                        # Check if user canceled
                        if user_cancel:
                            log_and_save('Application',
                                         {'Action': 'Cancelled', 'Message': 'User canceled operation'})
                            return excel_file_path

                        if it_component_name not in existing_it_components:
                            mutation4 = """
                                                                   mutation ($input: BaseFactSheetInput!, $patches: [Patch]!) {
                                       createFactSheet(input: $input, patches: $patches) {
                                         factSheet {
                                           name
                                           id
                                           type
                                           description
                                           tags {
                                             id
                                             name
                                           }
                                         }
                                       }
                                     }
                                                                   """
                            variables4 = {
                                "input": {
                                    "name": it_component_name,
                                    "type": "Application"
                                },
                                "patches": [
                                    {
                                        "op": "add",
                                        "path": "/description",
                                        "value": description
                                    },
                                    {
                                        "op": "add",
                                        "path": "/tags",
                                        "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'
                                    }
                                ]
                            }

                            data2 = {"query": mutation4, "variables": variables4}

                            response13 = requests.post(url=request_url, headers=headers, data=json.dumps(data2))
                            response13.raise_for_status()
                            appid = response13.json()
                            appp_id = appid['data']['createFactSheet']['factSheet']['id']
                            print(f"Application factsheet {it_component_name} Created")

                            # Log the creation action
                            logs['Application'].append({
                                'Name': it_component_name,
                                'Action': 'Created',
                                'FactSheet Type': 'Application',
                                'ID': appp_id

                            })

                # Fetch existing IT Components again
                existing_it_components2 = fetch_existing_it_components(request_url, headers)

                # Create client applications and log the creation
                create_client_application(clients_data, existing_it_components2, request_url, headers)

                # Save the logs for Client Applications
                # file_path = save_factsheet_logs_to_excel(logs, "created_client_applications", test_mode=False)

                # Fetch existing relationships for clients
                existing_relationships_query_clients = """
                                           {
                                             allFactSheets(factSheetType: Application) {
                                               edges {
                                                 node {
                                                   id
                                                   name
                                                   ... on Application {
                                                     relToChild {
                                                       edges {
                                                         node {
                                                           id
                                                           type
                                                           factSheet {
                                                             id
                                                             name
                                                           }
                                                         }
                                                       }
                                                     }
                                                   }
                                                 }
                                               }
                                             }
                                           }
                                           """

                response14 = requests.post(url=request_url, headers=headers,
                                           json={"query": existing_relationships_query_clients})
                response14.raise_for_status()
                existing_relationships_clients = response14.json()['data']['allFactSheets']['edges']

                existing_relationships_clients_set = set()
                for app in existing_relationships_clients:
                    parent_id = app['node']['id']
                    for relationship in app['node']['relToChild']['edges']:
                        child_id = relationship['node']['factSheet']['id']
                        existing_relationships_clients_set.add((parent_id, child_id))

                # Prepare unique relationship data before making the API calls
                relationship_data_clients = []

                for system in systems_data:
                    parent_name = f"{system['sid']} {system['product_version']}"
                    parent_id = applications.get(parent_name)
                    if parent_id:  # Ensure the parent application exists
                        for client in clients_data:
                            if system['sid'] == client['sid']:  # Ensure the client belongs to the same system
                                child_name = f"{client['sid']}-{client['client_id']} {client['product_version']}"
                                child_id = applications.get(child_name)
                                if child_id:  # Ensure the child application exists
                                    if (parent_id, child_id) not in existing_relationships_clients_set:
                                        relationship_data_clients.append({
                                            "Parent Application ID": parent_id,
                                            "Parent Application Name": parent_name,
                                            "Child Application ID": child_id,
                                            "Child Application Name": child_name
                                        })

                # Convert relationship data to a DataFrame
                relationship_df_clients = pd.DataFrame(relationship_data_clients)

                # Drop duplicates to ensure unique relationships
                relationship_df_clients = relationship_df_clients.drop_duplicates()

                # Proceed with the API calls to create the unique relationships
                for _, relationship in relationship_df_clients.iterrows():
                    parent_id = relationship["Parent Application ID"]
                    child_id = relationship["Child Application ID"]

                    relationship_mutation = f'''
                                               mutation {{
                                                 updateFactSheet(id: "{parent_id}", patches: [
                                                   {{
                                                     op: add,
                                                     path: "/relToChild/new_1",
                                                     value: "{{\\"factSheetId\\":\\"{child_id}\\"}}"
                                                   }}
                                                 ]) {{
                                                   factSheet {{
                                                     id
                                                     name
                                                     ... on Application {{
                                                       relToChild {{
                                                         edges {{
                                                           node {{
                                                             id
                                                           }}
                                                         }}
                                                       }}
                                                     }}
                                                   }}
                                                 }}
                                               }}
                                               '''
                    response15 = requests.post(url=request_url, headers=headers,
                                               data=json.dumps({"query": relationship_mutation}))
                    response15.raise_for_status()

                    print(
                        f"Created relationship between Parent Application '{relationship['Parent Application Name']}' and Child Application '{relationship['Child Application Name']}'")

                # ============================================= Modules ================================================================

                # Fetch existing factsheets
                def query_factsheet():
                    query = '''
                                           {
                                             allFactSheets(factSheetType: Application) {
                                               edges {
                                                 node {
                                                   id
                                                   name
                                                   displayName
                                                 }
                                               }
                                             }
                                           }
                                           '''
                    response16 = requests.post(request_url, headers=headers, json={'query': query})
                    response16.raise_for_status()  # Ensure any issues are raised
                    return response16.json()

                existing_factsheets_response = query_factsheet()
                existing_factsheets = {node['node']['name']: node['node'] for node in
                                       existing_factsheets_response['data']['allFactSheets']['edges']}

                existing_names = {node['node']['name'] for node in
                                  existing_factsheets_response['data']['allFactSheets']['edges']}

                # Function to create a new factsheet
                def create_factsheet(factsheet_name):
                    mutation5 = """
                                    mutation ($input: BaseFactSheetInput!, $patches: [Patch]!) {
                                        createFactSheet(input: $input, patches: $patches) {
                                            factSheet {
                                                name
                                                id
                                                type
                                                tags {
                                                    id
                                                    name
                                                }
                                            }
                                        }
                                    }
                                """
                    variables5 = {
                        "input": {
                            "name": factsheet_name,
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

                    data3 = {"query": mutation5, "variables": variables5}

                    response17 = requests.post(url=request_url, headers=headers, data=json.dumps(data3))
                    response17.raise_for_status()  # Ensure the request was successful
                    result = response17.json()

                    # Extract the ID of the newly created factsheet
                    factsheet_id = result['data']['createFactSheet']['factSheet']['id']
                    return factsheet_id

                # Iterate over module data to create or update factsheets
                for system in modules_data:
                    factsheet_name = system[
                        'belongs_to']  # Assume the 'belongs_to' key is the identifier for the factsheet
                    sub_module = system['acronym'] + "(" + system['sid'] + "-" + system['Client_id'] + ")"
                    if user_cancel:
                        log_and_save('Application', {'Action': 'Cancelled', 'Message': 'User canceled operation'})
                        return excel_file_path

                    if sub_module not in existing_names:
                        factsheet_id = create_factsheet(sub_module)
                        print(f"Application factsheet {sub_module} created with ID {factsheet_id}")

                        # Log the creation action including the ID
                        logs['Application'].append({
                            'Name': sub_module,
                            'Action': 'Created',
                            'FactSheet Type': 'Application',
                            'ID': factsheet_id  # Include the ID in the log
                        })

                # Fetch existing relationships for modules
                application_query = """{
                                             allFactSheets(factSheetType: Application) {
                                               edges {
                                                 node {
                                                   id
                                                   displayName
                                                   name
                                                 }
                                               }
                                             }
                                           }"""

                response22 = requests.post(url=request_url, headers=headers, json={"query": application_query})
                response22.raise_for_status()
                data_frame = pd.json_normalize(response22.json())

                applications = {node['node']['name'].strip(): node['node']['id'] for node in
                                response22.json()['data']['allFactSheets']['edges']}

                # Fetch existing relationships for modules
                existing_relationships_query_modules = """
                                           {
                                             allFactSheets(factSheetType: Application) {
                                               edges {
                                                 node {
                                                   id
                                                   name
                                                   ... on Application {
                                                     relToChild {
                                                       edges {
                                                         node {
                                                           id
                                                           type
                                                           factSheet {
                                                             id
                                                             name
                                                           }
                                                         }
                                                       }
                                                     }
                                                   }
                                                 }
                                               }
                                             }
                                           }
                                           """

                response16 = requests.post(url=request_url, headers=headers,
                                           json={"query": existing_relationships_query_modules})
                response16.raise_for_status()
                existing_relationships_modules = response16.json()['data']['allFactSheets']['edges']

                existing_relationships_modules_set = set()
                for app in existing_relationships_modules:
                    parent_id = app['node']['id']
                    for relationship in app['node']['relToChild']['edges']:
                        child_id = relationship['node']['factSheet']['id']
                        existing_relationships_modules_set.add((parent_id, child_id))

                # Prepare unique relationship data before making the API calls
                relationship_data_modules = []

                for client in clients_data:
                    parent_name = f"{client['sid']}-{client['client_id']} {client['product_version']}".strip()
                    parent_id = applications.get(parent_name)

                    if parent_id:  # Ensure the parent application exists
                        for module in modules_data:
                            if module['sid'] == client['sid'] and module['Client_id'] == client['client_id']:
                                child_name = f"{module['acronym']}({module['sid']}-{module['Client_id']})".strip()
                                child_id = applications.get(child_name)

                                if child_id:  # Ensure the child application exists
                                    if (parent_id, child_id) not in existing_relationships_modules_set:
                                        relationship_data_modules.append({
                                            "Parent Application ID": parent_id,
                                            "Parent Application Name": parent_name,
                                            "Child Application ID": child_id,
                                            "Child Application Name": child_name
                                        })

                # Convert relationship data to a DataFrame
                relationship_df_modules = pd.DataFrame(relationship_data_modules)

                # Drop duplicates to ensure unique relationships
                relationship_df_modules = relationship_df_modules.drop_duplicates()

                # Proceed with the API calls to create the unique relationships
                for _, relationship in relationship_df_modules.iterrows():
                    parent_id = relationship["Parent Application ID"]
                    child_id = relationship["Child Application ID"]

                    relationship_mutation = f'''
                                               mutation {{
                                                 updateFactSheet(id: "{parent_id}", patches: [
                                                   {{
                                                     op: add,
                                                     path: "/relToChild/new_1",
                                                     value: "{{\\"factSheetId\\":\\"{child_id}\\"}}"
                                                   }}
                                                 ]) {{
                                                   factSheet {{
                                                     id
                                                     name
                                                     ... on Application {{
                                                       relToChild {{
                                                         edges {{
                                                           node {{
                                                             id
                                                           }}
                                                         }}
                                                       }}
                                                     }}
                                                   }}
                                                 }}
                                               }}
                                               '''
                    response17 = requests.post(url=request_url, headers=headers,
                                               data=json.dumps({"query": relationship_mutation}))
                    response17.raise_for_status()

                    print(
                        f"Created relationship between Parent Application '{relationship['Parent Application Name']}' and Child Application '{relationship['Child Application Name']}'")

                # ======================================= Business Capability =========================================

                # Function to fetch existing Business Capability FactSheets
                def get_existing_business_capabilities():
                    query = """
                    {
                      allFactSheets(factSheetType: BusinessCapability) {
                        edges {
                          node {
                            id
                            name
                          }
                        }
                      }
                    }
                    """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    return {data['node']['name']: data['node']['id'] for data in
                            response.json()['data']['allFactSheets']['edges']}

                existing_business_capabilities = get_existing_business_capabilities()

                # Function to create a Business Capability FactSheet if it does not exist
                def create_business_capability(name):
                    mutation5 = """
                    mutation ($input: BaseFactSheetInput!, $patches: [Patch]!) {
                        createFactSheet(input: $input, patches: $patches) {
                            factSheet {
                                name
                                id
                                type
                                tags {
                                    id
                                    name
                                }
                            }
                        }
                    }
                    """
                    variables5 = {
                        "input": {
                            "name": name,
                            "type": "BusinessCapability"
                        },
                        "patches": [
                            {
                                "op": "add",
                                "path": "/tags",
                                "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'
                            }
                        ]
                    }

                    data3 = {"query": mutation5, "variables": variables5}

                    try:
                        response18 = requests.post(url=request_url, headers=headers, data=json.dumps(data3))
                        response18.raise_for_status()

                        response_json = response18.json()

                        # Check if 'data' and 'createFactSheet' are present in the response
                        factsheet_data = response_json.get('data', {}).get('createFactSheet')

                        if not factsheet_data or 'factSheet' not in factsheet_data:
                            print(f"Error: Missing 'factSheet' in response for {name}. Skipping this fact sheet.")
                            logs['BusinessCapability'].append({
                                'Name': name,
                                'Action': 'Error',
                                'Message': 'Missing factSheet in API response'
                            })
                            return  # Skip this fact sheet

                        factsheet = factsheet_data['factSheet']
                        factsheet_id = factsheet.get('id')

                        if not factsheet_id:
                            print(f"Error: 'id' missing in factSheet for {name}. Skipping this fact sheet.")
                            logs['BusinessCapability'].append({
                                'Name': name,
                                'Action': 'Error',
                                'Message': "FactSheet created but 'id' missing"
                            })
                            return  # Skip if there's no 'id'

                        print(f"Business Capability FactSheet {name} Created")

                        # Log the creation action and store the ID
                        logs['BusinessCapability'].append({
                            'Name': name,
                            'Action': 'Created',
                            'FactSheet Type': 'BusinessCapability',
                            'ID': factsheet_id
                        })
                        log_and_save('BusinessCapability', {
                            'Name': name,
                            'Action': 'Created',
                            'FactSheet Type': 'BusinessCapability',
                            'ID': factsheet_id
                        })

                    except (requests.exceptions.RequestException, KeyError, TypeError) as e:

                        logs['BusinessCapability'].append({
                            'Name': name,
                            'Action': 'Error',
                            'Message': str(e)
                        })

                # Function to process modules data, collect unique names, and create Business Capabilities
                def process_modules_data(modules_data1, existing_business_capabilities1):
                    # Collect all business capability names that need to be created
                    business_capability_names = [module['name'] for module in modules_data1 if
                                                 module['name'] not in existing_business_capabilities1]

                    # Remove duplicates by converting the list to a set and then back to a list
                    unique_business_capability_names = list(set(business_capability_names))

                    # Log the unique business capabilities for creation
                    print(f"Unique Business Capabilities to be created: {unique_business_capability_names}")

                    # Iterate over the unique business capability names and create them
                    for name in unique_business_capability_names:
                        if user_cancel:
                            log_and_save('BusinessCapability',
                                         {'Action': 'Cancelled', 'Message': 'User canceled operation'})
                            return excel_file_path

                        create_business_capability(name)

                # Reading the modules XML content
                modules_xml_content = read_xml_file(modules_xml_path)

                # Process modules data and log the creation
                process_modules_data(modules_data, existing_business_capabilities)

                def fetch_fact_sheets(fact_sheet_type):
                    query = f"""
                                           {{
                                             allFactSheets(factSheetType: {fact_sheet_type}) {{
                                               edges {{
                                                 node {{
                                                   id
                                                   name
                                                 }}
                                               }}
                                             }}
                                           }}
                                           """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    return {node['node']['name']: node['node']['id'] for node in
                            response.json()['data']['allFactSheets']['edges']}

                def create_relationship(app_id2, biz_cap_id, app_name, biz_cap_name):
                    mutation4 = f'''
                                               mutation {{
                                                 updateFactSheet(id: "{biz_cap_id}", patches: [
                                                   {{
                                                     op: add,
                                                     path: "/relBusinessCapabilityToApplication/new_1",
                                                     value: "{{\\"factSheetId\\":\\"{app_id2}\\"}}"
                                                   }}
                                                 ]) {{
                                                   factSheet {{
                                                     id
                                                     name
                                                     ... on BusinessCapability {{
                                                       relBusinessCapabilityToApplication {{
                                                         edges {{
                                                           node {{
                                                             id
                                                           }}
                                                         }}
                                                       }}
                                                     }}
                                                   }}
                                                 }}
                                               }}
                                               '''

                    response = requests.post(url=request_url, headers=headers, json={"query": mutation4})
                    response.raise_for_status()
                    print(
                        f"Created relationship from Application '{app_name}' to Business Capability '{biz_cap_name}'")

                # Fetch all applications and business capabilities
                applications = fetch_fact_sheets("Application")
                business_capabilities = fetch_fact_sheets("BusinessCapability")

                # Fetch existing relationships
                existing_relationships_query_bc = """
                                       {
                                         allFactSheets(factSheetType: BusinessCapability) {
                                           edges {
                                             node {
                                               id
                                               name
                                               ... on BusinessCapability {
                                                 relBusinessCapabilityToApplication {
                                                   edges {
                                                     node {
                                                       id
                                                       factSheet {
                                                         id
                                                         name
                                                       }
                                                     }
                                                   }
                                                 }
                                               }
                                             }
                                           }
                                         }
                                       }
                                       """

                response_bc = requests.post(url=request_url, headers=headers,
                                            json={"query": existing_relationships_query_bc})
                response_bc.raise_for_status()
                existing_relationships_bc = response_bc.json()['data']['allFactSheets']['edges']

                existing_relationships_bc_set = set()
                for bc in existing_relationships_bc:
                    bc_id = bc['node']['id']
                    for relationship in bc['node']['relBusinessCapabilityToApplication']['edges']:
                        app_id = relationship['node']['factSheet']['id']
                        existing_relationships_bc_set.add((bc_id, app_id))

                # Prepare unique relationship data before making the API calls
                relationship_data_bc = []

                for module in modules_data:
                    app_name = f"{module['acronym']}({module['sid']}-{module['Client_id']})".strip()
                    app_id = applications.get(app_name)
                    if app_id:
                        for bc_name, bc_id in business_capabilities.items():
                            if bc_name == module['name']:
                                if (bc_id, app_id) not in existing_relationships_bc_set:
                                    relationship_data_bc.append({
                                        "Business Capability ID": bc_id,
                                        "Business Capability Name": bc_name,
                                        "Application ID": app_id,
                                        "Application Name": app_name
                                    })

                # Convert relationship data to a DataFrame
                relationship_df_bc = pd.DataFrame(relationship_data_bc)

                # Drop duplicates to ensure unique relationships
                relationship_df_bc = relationship_df_bc.drop_duplicates()

                # Proceed with the API calls to create the unique relationships
                for _, relationship in relationship_df_bc.iterrows():
                    bc_id = relationship["Business Capability ID"]
                    app_id = relationship["Application ID"]
                    bc_name = relationship["Business Capability Name"]  # Passing the business capability name
                    app_name = relationship["Application Name"]  # Passing the application name
                    create_relationship(app_id, bc_id, app_name, bc_name)

                # # ==================================================== Data Objects ======================

                # Load the Excel data
                file_path = 'data_object.xlsx'
                excel_data = pd.read_excel(file_path)

                # Extract the IDOC message types and their corresponding Langtext_EN
                idoc_data = excel_data[['IDOC_Message_Type', 'Langtext_EN']].to_dict(orient='records')

                # Fetch existing Data Objects
                def get_existing_data_objects():
                    query = """
                                                                            {
                                                                              allFactSheets(factSheetType: DataObject) {
                                                                                edges {
                                                                                  node {
                                                                                    id
                                                                                    name
                                                                                  }
                                                                                }
                                                                              }
                                                                            }
                                                                            """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    return {data['node']['name']: data['node']['id'] for data in
                            response.json()['data']['allFactSheets']['edges']}

                existing_data_objects = get_existing_data_objects()

                def create_data_object(name, existing_data_objects):
                    if name not in existing_data_objects:
                        mutation6 = """
                                                                                mutation ($input: BaseFactSheetInput!, $patches: [Patch]!) {
                                                                                  createFactSheet(input: $input, patches: $patches) {
                                                                                    factSheet {
                                                                                      name
                                                                                      id
                                                                                      tags {
                                                                                        id
                                                                                        name
                                                                                      }
                                                                                    }
                                                                                  }
                                                                                }
                                                                                """
                        variables6 = {
                            "input": {
                                "name": name,
                                "type": "DataObject"
                            },
                            "patches": [
                                {
                                    "op": "add",
                                    "path": "/tags",
                                    "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'
                                }
                            ],
                            "tagName": tag_name
                        }

                        data4 = {"query": mutation6, "variables": variables6}

                        response18 = requests.post(url=request_url, headers=headers, data=json.dumps(data4))
                        response18.raise_for_status()
                        result = response18.json()
                        new_id = result['data']['createFactSheet']['factSheet']['id']
                        existing_data_objects[name] = new_id
                        print(f"Data Object FactSheet {name} created")
                        logs['DataObject'].append({
                            "Action": "Created",
                            "FactSheet Type": "DataObject",
                            "Name": name,
                            'ID': new_id
                        })

                # Process ALE data to create Data Object FactSheets
                def process_ale_data(ale_data, idoc_data, existing_data_objects):
                    idoc_dict = {item['IDOC_Message_Type']: item['Langtext_EN'] for item in idoc_data}
                    for ale_item in ale_data:
                        idoc_type = ale_item['idoc_messagetype']
                        if user_cancel:
                            log_and_save('DataObject',
                                         {'Action': 'Cancelled', 'Message': 'User canceled operation'})
                            return excel_file_path

                        if idoc_type in idoc_dict:
                            langtext_en = idoc_dict[idoc_type]

                            create_data_object(langtext_en, existing_data_objects)

                # Process the ALE data
                process_ale_data(Ale_data, idoc_data, existing_data_objects)

                # ===================================== Interface ============================================

                # Fetch all existing interfaces
                def fetch_all_interfaces():
                    query = """
                                           query {
                                             allFactSheets(factSheetType: Interface) {
                                               edges {
                                                 node {
                                                   id
                                                   name
                                                 }
                                               }
                                             }
                                           }
                                           """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    result = response.json()
                    return {edge['node']['name'].lower(): edge['node']['id'] for edge in
                            result['data']['allFactSheets']['edges']}

                existing_interfaces = fetch_all_interfaces()

                # Function to calculate similarity between two strings
                def similar(a, b):
                    if a is None or b is None:
                        return 0
                    return SequenceMatcher(None, a, b).ratio()

                # Match RFC data with Client data and Systems data
                def match_rfc_to_clients_and_systems(rfc_data, clients_data, systems_data):
                    matched_data4 = []
                    for rfc in rfc_data:
                        matched = False
                        for client in clients_data:
                            if similar(rfc['rfc_destination'], client['logical_system_name']) > 0.9:
                                sender = f"{rfc['sid']} {rfc['product_version']}"
                                receiver = f"{rfc['sid']}-{client['client_id']} {rfc['product_version']}"
                                if sender != receiver:
                                    interface_name = f"{sender} ->> {receiver}"
                                    matched_data4.append({
                                        'Interface_Name': interface_name,
                                        'Sender': sender,
                                        'Receiver': receiver
                                    })
                                matched = True
                                break
                        if not matched:
                            for system in systems_data:
                                if similar(rfc['target'], system['host_ip']) > 0.9:
                                    sender = f"{rfc['sid']} {rfc['product_version']}"
                                    receiver = f"{system['sid']} {system['product_version']}"
                                    if sender != receiver:
                                        interface_name = f"{sender} ->> {receiver}"
                                        matched_data4.append({
                                            'Interface_Name': interface_name,
                                            'Sender': sender,
                                            'Receiver': receiver
                                        })
                                    matched = True
                                    break
                        if not matched:
                            for system in systems_data:
                                if similar(rfc['target'], system['host_name']) > 0.9:
                                    sender = f"{rfc['sid']} {rfc['product_version']}"
                                    receiver = f"{system['sid']} {system['product_version']}"
                                    if sender != receiver:
                                        interface_name = f"{sender} ->> {receiver}"
                                        matched_data4.append({
                                            'Interface_Name': interface_name,
                                            'Sender': sender,
                                            'Receiver': receiver
                                        })
                                    break
                    return matched_data4

                # Assuming Rfc_data, clients_data, systems_data are defined elsewhere in your script
                matched_data = match_rfc_to_clients_and_systems(Rfc_data, clients_data, systems_data)

                # Convert matched data to DataFrame and drop duplicates
                matched_df = pd.DataFrame(matched_data)
                matched_df = matched_df.drop_duplicates()

                # Helper function to create interface fact sheets
                def create_interface_rfc_factsheet(interface, existing_interfaces):
                    interface_name_lower = interface['Interface_Name'].lower()
                    if interface_name_lower not in existing_interfaces:
                        mutation74 = """
                                               mutation ($input: BaseFactSheetInput!, $patches: [Patch]!) {
                                                 createFactSheet(input: $input, patches: $patches) {
                                                   factSheet {
                                                     name
                                                     id
                                                     type
                                                     tags {
                                                       id
                                                       name
                                                     }
                                                   }
                                                 }
                                               }
                                               """
                        variables74 = {
                            "input": {
                                "name": interface['Interface_Name'],
                                "type": "Interface"
                            },
                            "patches": [
                                {
                                    "op": "add",
                                    "path": "/tags",
                                    "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'
                                }
                            ]
                        }

                        data74 = {"query": mutation74, "variables": variables74}
                        response = requests.post(url=request_url, headers=headers, json=data74)
                        response.raise_for_status()
                        result = response.json()
                        if 'errors' in result:
                            print(f"Error creating Interface FactSheet: {result['errors']}")
                        else:
                            new_interface = result['data']['createFactSheet']['factSheet']
                            existing_interfaces[interface_name_lower] = new_interface['id']

                            # Log the creation action
                            logs['Interface'].append({
                                'Name': new_interface['name'],
                                'Action': 'Created',
                                'FactSheet Type': 'Interface',
                                'ID': new_interface['id']
                            })
                            print(f" RFC Interface factsheet {interface['Interface_Name']} created")

                # Iterate through the matched interfaces and create fact sheets if they do not exist
                for _, interface in matched_df.iterrows():
                    existing_interfaces = fetch_all_interfaces()
                    if user_cancel:
                        log_and_save('Interface', {'Action': 'Cancelled', 'Message': 'User canceled operation'})
                        return excel_file_path
                    # Refresh the existing interfaces before each creation attempt
                    create_interface_rfc_factsheet(interface, existing_interfaces)

                # print("RFC Done")

                # ALE Function to create an interface fact sheet
                def create_interface_factsheet(interface_name, existing_interfaces):
                    interface_name_lower = interface_name.lower()
                    if interface_name_lower not in existing_interfaces:
                        mutation55 = """
                                               mutation ($input: BaseFactSheetInput!, $patches: [Patch]!) {
                                                 createFactSheet(input: $input, patches: $patches) {
                                                   factSheet {
                                                     name
                                                     id
                                                     type
                                                     tags {
                                                       id
                                                       name
                                                     }
                                                   }
                                                 }
                                               }
                                               """
                        variables55 = {
                            "input": {
                                "name": interface_name,
                                "type": "Interface"
                            },
                            "patches": [
                                {
                                    "op": "add",
                                    "path": "/tags",
                                    "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'
                                }
                            ]
                        }

                        data55 = {"query": mutation55, "variables": variables55}
                        response = requests.post(url=request_url, headers=headers, json=data55)
                        response.raise_for_status()
                        result = response.json()
                        if 'errors' in result:
                            print(f"Error creating Interface FactSheet: {result['errors']}")
                        else:
                            new_interface = result['data']['createFactSheet']['factSheet']
                            print(f" ALE Interface FactSheet {new_interface['name']} Created")
                            existing_interfaces[interface_name_lower] = new_interface['id']

                            # Log the creation action
                            logs['Interface'].append({
                                'Name': new_interface['name'],
                                'Action': 'Created',
                                'FactSheet Type': 'Interface',
                                'ID': new_interface['id']
                            })

                # Helper function to create interface details from ALE data
                def create_interfaces(clients_data2, ale_data2):
                    interfaces5 = []
                    for ale in ale_data2:
                        sender_matches = [client for client in clients_data2 if
                                          client['logical_system_name'] == ale['sender']]
                        receiver_matches = [client for client in clients_data2 if
                                            client['logical_system_name'] == ale['receiver']]
                        for sender in sender_matches:
                            for receiver in receiver_matches:
                                interface_name = f"{sender['sid']}-{sender['client_id']} {sender['product_version']} ->> {receiver['sid']}-{receiver['client_id']} {receiver['product_version']}"
                                interfaces5.append({
                                    "interface_name": interface_name,
                                    "idoc_messagetype": ale['idoc_messagetype']
                                })
                    return interfaces5

                # Assuming clients_data and Ale_data are defined elsewhere in your script
                interfaces = create_interfaces(clients_data, Ale_data)

                # Convert interfaces to DataFrame and drop duplicates
                matched_df2 = pd.DataFrame(interfaces)
                matched_df2 = matched_df2.drop_duplicates()

                for _, interface in matched_df2.iterrows():
                    if user_cancel:
                        log_and_save('Interface', {'Action': 'Cancelled', 'Message': 'User canceled operation'})
                        return excel_file_path
                    create_interface_factsheet(interface['interface_name'], existing_interfaces)

                # print("ALE Done")

                # Fetch all fact sheets of a given type
                def fetch_all_fact_sheets(fact_sheet_type):
                    query = f"""
                                                   {{
                                                     allFactSheets(factSheetType: {fact_sheet_type}) {{
                                                       edges {{
                                                         node {{
                                                           id
                                                           name
                                                         }}
                                                       }}
                                                     }}
                                                   }}
                                                   """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    return {node['node']['name']: node['node']['id'] for node in
                            response.json()['data']['allFactSheets']['edges']}

                # Fetch all Data Objects and Interfaces again to ensure we have the latest IDs
                data_objects = fetch_all_fact_sheets("DataObject")
                interfaces = fetch_all_fact_sheets("Interface")

                # Create relationships between Data Objects and Interfaces
                def create_relationship_interface(data_object_id, data_object_name, interface_id,
                                                  interface_name):
                    if data_object_id and interface_id:
                        mutation = f'''
                                                       mutation {{
                                                         updateFactSheet(id: "{data_object_id}", patches: [
                                                           {{
                                                             op: add,
                                                             path: "/relDataObjectToInterface/new_1",
                                                             value: "{{\\"factSheetId\\":\\"{interface_id}\\"}}"
                                                           }}
                                                         ]) {{
                                                           factSheet {{
                                                             id
                                                             ... on DataObject {{
                                                               relDataObjectToInterface {{
                                                                 edges {{
                                                                   node {{
                                                                     id
                                                                   }}
                                                                 }}
                                                               }}
                                                             }}
                                                           }}
                                                         }}
                                                       }}
                                                       '''
                        response = requests.post(url=request_url, headers=headers, json={"query": mutation})
                        if response.ok:
                            print(
                                f"Created relationship from Data Object '{data_object_name}' to Interface '{interface_name}'")
                        else:
                            print(
                                f"Failed to create relationship between Data Object '{data_object_name}' and Interface '{interface_name}': {response.text}")

                # Fetch existing relationships (Data Object to Interface)
                def fetch_existing_relationships_data_object_to_interface():
                    query = """
                                                   {
                                                     allFactSheets(factSheetType: DataObject) {
                                                       edges {
                                                         node {
                                                           id
                                                           name
                                                           ... on DataObject {
                                                             relDataObjectToInterface {
                                                               edges {
                                                                 node {
                                                                   id
                                                                   factSheet {
                                                                     id
                                                                     name
                                                                   }
                                                                 }
                                                               }
                                                             }
                                                           }
                                                         }
                                                       }
                                                     }
                                                   }
                                                   """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    result = response.json()
                    relationships = set()
                    for edge in result['data']['allFactSheets']['edges']:
                        data_object_id = edge['node']['id']
                        for rel in edge['node']['relDataObjectToInterface']['edges']:
                            interface_id = rel['node']['factSheet']['id']
                            relationships.add((data_object_id, interface_id))
                    return relationships

                existing_relationships_data_object_to_interface = fetch_existing_relationships_data_object_to_interface()

                # Prepare relationship data between Data Objects and Interfaces
                relationship_data_object_to_interface = []
                idoc_dict = {item['IDOC_Message_Type']: item['Langtext_EN'] for item in idoc_data}

                for ale in Ale_data:
                    data_object_name = idoc_dict.get(ale['idoc_messagetype'])
                    if data_object_name:
                        data_object_id = data_objects.get(data_object_name)
                        sender_matches = [client for client in clients_data if
                                          client['logical_system_name'] == ale['sender']]
                        receiver_matches = [client for client in clients_data if
                                            client['logical_system_name'] == ale['receiver']]
                        for sender in sender_matches:
                            for receiver in receiver_matches:
                                interface_name = f"{sender['sid']}-{sender['client_id']} {sender['product_version']} ->> {receiver['sid']}-{receiver['client_id']} {receiver['product_version']}"
                                interface_id = interfaces.get(interface_name)
                                if data_object_id and interface_id and (
                                        data_object_id,
                                        interface_id) not in existing_relationships_data_object_to_interface:
                                    relationship_data_object_to_interface.append({
                                        "Data Object ID": data_object_id,
                                        "Interface ID": interface_id,
                                        "Data Object Name": data_object_name,
                                        "Interface Name": interface_name
                                    })

                # Convert relationship data to a DataFrame
                relationship_df_data_object_to_interface = pd.DataFrame(relationship_data_object_to_interface)
                relationship_df_data_object_to_interface = relationship_df_data_object_to_interface.drop_duplicates()

                # Proceed with the API calls to create the unique relationships
                for _, relationship in relationship_df_data_object_to_interface.iterrows():
                    create_relationship_interface(relationship["Data Object ID"],
                                                  relationship["Data Object Name"],
                                                  relationship["Interface ID"], relationship["Interface Name"])

                # # Fetch all applications
                def fetch_all_applications():
                    query = """
                                                   {
                                                     allFactSheets(factSheetType: Application) {
                                                       edges {
                                                         node {
                                                           id
                                                           name
                                                         }
                                                       }
                                                     }
                                                   }
                                                   """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    return {node['node']['name']: node['node']['id'] for node in
                            response.json()['data']['allFactSheets']['edges']}

                applications = fetch_all_applications()

                # Create relationships between Interfaces and Applications
                def create_application_interface_relationship(interface_id, provider_id, consumer_id,
                                                              interface_name,
                                                              provider_name, consumer_name):
                    mutation11 = f'''
                                                       mutation {{
                                                         updateFactSheet(id: "{interface_id}", patches: [
                                                           {{
                                                             op: add,
                                                             path: "/relInterfaceToConsumerApplication/new_1",
                                                             value: "{{\\"factSheetId\\":\\"{consumer_id}\\"}}"
                                                           }}, {{
                                                             op: add,
                                                             path: "/relInterfaceToProviderApplication/new",
                                                             value: "{{\\"factSheetId\\":\\"{provider_id}\\"}}"
                                                           }}
                                                         ]) {{
                                                           factSheet {{
                                                             id
                                                             ... on Interface {{
                                                               relInterfaceToConsumerApplication {{
                                                                 edges {{
                                                                   node {{
                                                                     id
                                                                   }}
                                                                 }}
                                                               }},
                                                               relInterfaceToProviderApplication {{
                                                                 edges {{
                                                                   node {{
                                                                     id
                                                                   }}
                                                                 }}
                                                               }}
                                                             }}
                                                           }}
                                                         }}
                                                       }}
                                                       '''
                    response = requests.post(url=request_url, headers=headers, json={"query": mutation11})
                    response.raise_for_status()
                    if response.ok:
                        print(
                            f"Relationships created for Interface '{interface_name}' with Provider interface '{provider_name}' and Consumer interface '{consumer_name}'")
                    else:
                        print(f"Failed to create relationships: {response.text}")

                # Fetch existing relationships (Interface to Application)
                def fetch_existing_relationships_interface_to_application():
                    query = """
                                                   {
                                                     allFactSheets(factSheetType: Interface) {
                                                       edges {
                                                         node {
                                                           id
                                                           name
                                                           ... on Interface {
                                                             relInterfaceToConsumerApplication {
                                                               edges {
                                                                 node {
                                                                   id
                                                                   factSheet {
                                                                     id
                                                                     name
                                                                   }
                                                                 }
                                                               }
                                                             }
                                                             relInterfaceToProviderApplication {
                                                               edges {
                                                                 node {
                                                                   id
                                                                   factSheet {
                                                                     id
                                                                     name
                                                                   }
                                                                 }
                                                               }
                                                             }
                                                           }
                                                         }
                                                       }
                                                     }
                                                   }
                                                   """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    result = response.json()
                    relationships = set()
                    for edge in result['data']['allFactSheets']['edges']:
                        interface_id = edge['node']['id']
                        for rel in edge['node']['relInterfaceToConsumerApplication']['edges']:
                            consumer_id = rel['node']['factSheet']['id']
                            relationships.add((interface_id, consumer_id))
                        for rel in edge['node']['relInterfaceToProviderApplication']['edges']:
                            provider_id = rel['node']['factSheet']['id']
                            relationships.add((interface_id, provider_id))
                    return relationships

                existing_relationships_interface_to_application = fetch_existing_relationships_interface_to_application()

                # Prepare relationship data for Interfaces and Applications
                relationship_interface_to_application = []
                for interface_name, interface_id in interfaces.items():
                    split_name = interface_name.split(" ->> ")
                    if len(split_name) == 2:
                        provider_name = split_name[0].strip()
                        consumer_name = split_name[1].strip()
                        provider_id = applications.get(provider_name)
                        consumer_id = applications.get(consumer_name)
                        if provider_id and consumer_id and (
                                interface_id,
                                provider_id) not in existing_relationships_interface_to_application and (
                                interface_id,
                                consumer_id) not in existing_relationships_interface_to_application:
                            relationship_interface_to_application.append({
                                "Interface ID": interface_id,
                                "Provider ID": provider_id,
                                "Consumer ID": consumer_id,
                                "Interface Name": interface_name,
                                "Provider Name": provider_name,
                                "Consumer Name": consumer_name
                            })

                # Convert relationship data to a DataFrame
                relationship_df_interface_to_application = pd.DataFrame(relationship_interface_to_application)

                # Drop duplicates to ensure unique relationships
                relationship_df_interface_to_application = relationship_df_interface_to_application.drop_duplicates()

                # Proceed with the API calls to create the unique relationships
                for _, relationship in relationship_df_interface_to_application.iterrows():
                    create_application_interface_relationship(
                        relationship["Interface ID"],
                        relationship["Provider ID"],
                        relationship["Consumer ID"],
                        relationship["Interface Name"],
                        relationship["Provider Name"],
                        relationship["Consumer Name"]
                    )



        except Exception as e:
            print(f"An error occurred: {e}")
            save_factsheet_logs_to_excel(logs)
            raise

        # After creating all applications and IT Components, save the logs to an Excel file
        file_path5 = save_factsheet_logs_to_excel(logs)
        return file_path5

    if delta_operation:
        os.makedirs(operation_folder, exist_ok=True)

        def save_factsheet_logs_to_excel2(logs, file_prefix, test_mode=False):
            # Combine logs into a single DataFrame
            combined_logs = []
            for factsheet_type, log_entries in logs.items():
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

            timestamp = datetime.now().strftime('%d-%m-%Y - %H-%M')

            if test_mode:
                # Create a hidden directory for test mode
                hidden_dir = os.path.join(os.path.expanduser('~'), '.hidden_test_mode_logs')
                os.makedirs(hidden_dir, exist_ok=True)

                file_path = os.path.join(hidden_dir, f"{file_prefix}.xlsx")

                # Save the file to the hidden directory
                with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
                    combined_df.to_excel(writer, sheet_name='Logs', index=False)

                return file_path
            else:
                # Save to the path_to_output_directory in normal mode
                output_dir = os.path.join('path_to_output_directory', timestamp)
                os.makedirs(output_dir, exist_ok=True)
                file_path = os.path.join(output_dir, f"{file_prefix}.xlsx")

                with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
                    combined_df.to_excel(writer, sheet_name='Logs', index=False)

                return file_path

        def save_created_updated_factsheets_to_excel(created_factsheets, updated_factsheets, deleted_factsheets):
            # Safeguard against None values in the created, updated, and deleted factsheets
            created_factsheets = [
                {k: (v if v is not None else '') for k, v in factsheet.items()}
                for factsheet in created_factsheets
            ]

            updated_factsheets = [
                {k: (v if v is not None else '') for k, v in factsheet.items()}
                for factsheet in updated_factsheets
            ]

            deleted_factsheets = [
                {k: (v if v is not None else '') for k, v in factsheet.items()}
                for factsheet in deleted_factsheets
            ]

            # Combine created, updated, and deleted factsheets into separate DataFrames
            created_df = pd.DataFrame(created_factsheets).drop_duplicates(subset='Name', keep='first')
            updated_df = pd.DataFrame(updated_factsheets).drop_duplicates(subset='Name', keep='first')
            deleted_df = pd.DataFrame(deleted_factsheets).drop_duplicates(subset='Name', keep='first')

            timestamp = datetime.now().strftime('%d-%m-%Y - %H-%M')

            # Save to the path_to_output_directory in normal mode
            output_dir = os.path.join('path_to_output_directory', timestamp)
            os.makedirs(output_dir, exist_ok=True)

            created_file_path = os.path.join(output_dir, "created_factsheets.xlsx")
            updated_file_path = os.path.join(output_dir, "updated_factsheets.xlsx")
            deleted_file_path = os.path.join(output_dir, "deleted_factsheets.xlsx")

            # Save each DataFrame into its respective file
            with pd.ExcelWriter(created_file_path, engine='xlsxwriter') as writer:
                created_df.to_excel(writer, sheet_name='CreatedFactsheets', index=False)

            with pd.ExcelWriter(updated_file_path, engine='xlsxwriter') as writer:
                updated_df.to_excel(writer, sheet_name='UpdatedFactsheets', index=False)

            with pd.ExcelWriter(deleted_file_path, engine='xlsxwriter') as writer:
                deleted_df.to_excel(writer, sheet_name='DeletedFactsheets', index=False)

            return created_file_path, updated_file_path, deleted_file_path

        created_factsheets = []
        updated_factsheets = []
        deleted_factsheets = []
        try:

            for _ in range(3):  # Loop to run the process three times
                if user_cancel:
                    print("User canceled operation before starting. Saving logs up to this point...")
                    save_factsheet_logs_to_excel(logs)
                    return excel_file_path

                application_level1 = []
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
                                    in
                                    systems_data}
                normalized_xml_applications = {normalize_name(app): app for app in xml_applications}
                applications_to_create = {app: xml_applications[app] for norm_app, app in
                                          normalized_xml_applications.items() if
                                          norm_app not in existing_app_names}
                # Extract and normalize XML applications
                xml_applications = {f"{system['sid']} {system['product_version']}": system['description'] for system
                                    in
                                    systems_data}
                normalized_xml_applications = {normalize_name(app): app for app in xml_applications}

                for app_name, description in xml_applications.items():
                    normalized_app_name = normalize_name(app_name)
                    if user_cancel:
                        print("User canceled operation before starting. Saving logs up to this point...")
                        save_factsheet_logs_to_excel(logs)
                        return excel_file_path

                    if normalized_app_name in existing_app_names:
                        # Application already exists, so just log it
                        existing_app = existing_app_names[normalized_app_name]
                        application_level1.append({
                            "name": existing_app['displayName'],
                            "id": existing_app['id'],
                            "description": existing_app['description']
                        })

                    else:
                        # Application does not exist, so create it
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
                                },
                                {
                                    "op": "add",
                                    "path": "/description",
                                    "value": description
                                }
                            ],
                            "tagName": tag_name
                        }
                        data = {"query": mutation, "variables": variables}
                        response21 = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                        response21.raise_for_status()
                        response_data_create = response21.json()

                        # Extract application ID and log the creation action
                        application_id = response_data_create['data']['createFactSheet']['factSheet']['id']
                        logs['Application'].append({
                            'Action': 'Created',
                            'Name': app_name,
                            'FactSheet Type': 'Application',
                            "ID": application_id
                        })

                        application_level1.append({
                            "name": app_name,
                            "id": application_id,
                            "description": description
                        })

                        created_factsheets.append({
                            "Name": app_name,
                            "Description": description,
                            "FactSheet Type": "Application",
                            "ID": application_id
                        })
                        # Log the creation action
                        log_and_save('Application', {
                            'Action': 'Created',
                            'Name': app_name,
                            'FactSheet Type': 'Application',
                            'ID': application_id  # Include the ID in the log
                        })
                        print(f"Application factsheet {app_name} created")

                # Update existing applications
                for norm_app_name, app_name in normalized_xml_applications.items():
                    if user_cancel:
                        print("User canceled operation before starting. Saving logs up to this point...")
                        save_factsheet_logs_to_excel(logs)
                        return excel_file_path
                    if norm_app_name in existing_app_names:
                        existing_app = existing_app_names[norm_app_name]
                        application_id = existing_app['id']
                        current_description = existing_app['description']
                        new_description = xml_applications[app_name]

                        # Check if the description has changed before updating
                        if current_description != new_description:
                            mutation = """
                                                    mutation ($id: ID!, $patches: [Patch]!) {
                                                        updateFactSheet(id: $id, patches: $patches) {
                                                            factSheet {
                                                                id
                                                                name
                                                                description
                                                            }
                                                        }
                                                    }
                                                    """
                            variables = {
                                "id": application_id,
                                "patches": [
                                    {"op": "replace", "path": "/description", "value": new_description}
                                ]
                            }
                            data = {"query": mutation, "variables": variables}
                            response22 = requests.post(request_url, headers=headers, data=json.dumps(data))
                            response22.raise_for_status()
                            print(
                                f"Application factsheet updated: {app_name} with new description: {new_description}")

                            logs['Application'].append({
                                'Action': 'Updated',
                                'Name': app_name,
                                'FactSheet Type': 'Application',
                                "description": new_description,
                                "ID": application_id
                            })
                            application_level1.append({
                                "name": app_name,
                                "id": application_id,
                                "description": new_description
                            })
                            updated_factsheets.append({
                                "Name": app_name,
                                "Description": new_description,
                                "FactSheet Type": "Application",
                                "ID": application_id
                            })
                            log_and_save('Application', {
                                'Action': 'Created',
                                'Name': app_name,
                                'FactSheet Type': 'Application',
                                'ID': application_id  # Include the ID in the log
                            })

                # IT Components

                component_query = """
                                        {
                                          allFactSheets(factSheetType: ITComponent) {
                                            edges {
                                              node {
                                                id
                                                displayName
                                                name
                                                description
                                                ... on ITComponent {
                                                      release
                                                      name
                                                }
                                                tags {
                                                    name
                                                }
                                                category
                                              }
                                            }
                                          }
                                        }
                                        """
                response3 = requests.post(url=request_url, headers=headers, json={"query": component_query})
                response3.raise_for_status()

                # Check if the response contains errors
                response_data = response3.json()
                if 'errors' in response_data:
                    print("Error in response:", response_data['errors'])
                    leanix_it_components = []
                else:
                    leanix_it_components_data = response_data.get('data', {}).get('allFactSheets', {}).get('edges',
                                                                                                           [])
                    if leanix_it_components_data is None:
                        leanix_it_components_data = []
                    leanix_it_components = [node['node'] for node in leanix_it_components_data]

                # Normalize function for comparison
                def normalize_name(name):
                    return unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii').lower()

                # Function to normalize and compare descriptions
                def normalize_description(description):
                    return description.strip() if description else ''

                # Function to check for changes and update IT Component
                def update_it_component_if_changed(component_id, display_name, new_description):
                    mutation = """
                                            mutation ($id: ID!, $patches: [Patch]!) {
                                                updateFactSheet(id: $id, patches: $patches) {
                                                    factSheet {
                                                        id
                                                        name
                                                        description
                                                    }
                                                }
                                            }
                                            """
                    variables = {
                        "id": component_id,
                        "patches": []
                    }

                    if new_description is not None:
                        variables["patches"].append({
                            "op": "replace",
                            "path": "/description",
                            "value": new_description
                        })

                    data = {"query": mutation, "variables": variables}
                    response = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                    response.raise_for_status()
                    return True

                # Function to archive IT Component
                def archive_it_component(component_id, display_name):
                    mutation = """
                                            mutation ($id: ID!) {
                                                updateFactSheet(id: $id, comment: "Irrelevant IT Component", patches: [
                                                    {
                                                        op: add,
                                                        path: "/status",
                                                        value: "ARCHIVED"
                                                    }
                                                ]) {
                                                    factSheet {
                                                        id
                                                        status
                                                    }
                                                }
                                            }
                                            """
                    variables = {
                        "id": component_id
                    }

                    data = {"query": mutation, "variables": variables}
                    response = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                    response.raise_for_status()
                    print(f"IT Component facsheet {display_name} deleted")
                    return True

                # Prepare a set of new IT components for comparison
                new_it_components = {f"{component['name']} {component['release']}" for component in components_data}

                # Compare and update or create IT Components if there are changes
                existing_it_component_names = {f"{it['name']} {it['release']}" for it in leanix_it_components}

                for component in components_data:
                    if user_cancel:
                        print("User canceled operation before starting. Saving logs up to this point...")
                        save_factsheet_logs_to_excel(logs)
                        return excel_file_path
                    component_display_name = f"{component['name']} {component['release']}"
                    existing_component = next(
                        (it for it in leanix_it_components if it['displayName'] == component_display_name), None
                    )

                    if existing_component:
                        component_id = existing_component['id']
                        current_description = normalize_description(existing_component.get('description', ''))
                        new_description = normalize_description(component['description'])

                        # Check if the current description differs from the new description
                        if current_description != new_description:
                            if update_it_component_if_changed(component_id, component_display_name,
                                                              new_description):
                                logs['ITComponent'].append({
                                    "Action": "Updated",
                                    "FactSheet Type": "ITComponent",
                                    "Name": component['name'],
                                    "Description": new_description,
                                    "ID": component_id
                                })
                                updated_factsheets.append({
                                    "Name": component['name'],
                                    "Description": new_description,
                                    "FactSheet Type": "ITComponent",
                                    "ID": component_id
                                })
                                log_and_save('ITComponent', {
                                    'Action': 'Updated',
                                    "FactSheet Type": "ITComponent",
                                    "Name": component['name'],
                                    "Description": new_description,
                                    "ID": component_id
                                })

                    else:
                        # Wrap the creation process in a try-except block to handle errors
                        try:
                            # Create missing IT Components
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
                                    "name": component['name'],
                                    "type": "ITComponent"
                                },
                                "patches": [
                                    {
                                        "op": "add",
                                        "path": "/description",
                                        "value": component['description']
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
                                        "value": component['release']
                                    }
                                ],
                                "tagName": tag_name
                            }

                            data = {"query": mutation, "variables": variables}
                            response5 = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                            response5.raise_for_status()  # Raises an HTTPError if the response is an error
                            response_data_create = response5.json()
                            it_id = response_data_create['data']['createFactSheet']['factSheet']['id']
                            print(f"IT Component factsheet {component_display_name} created")

                            # Log the creation
                            logs['ITComponent'].append({
                                "Action": "Created",
                                "FactSheet Type": "ITComponent",
                                "Name": component['name'],
                                "Description": component['description'],
                                "ID": it_id
                            })
                            created_factsheets.append({
                                "Name": component['name'],
                                "Description": component['description'],
                                "FactSheet Type": "ITComponent",
                                "ID": it_id
                            })
                            log_and_save('ITComponent', {
                                'Action': 'Created',
                                "FactSheet Type": "ITComponent",
                                "Name": component['name'],
                                "ID": it_id
                            })

                        except Exception as e:
                            # Log the error and move to the next component
                            # print(f"Failed to create IT Component factsheet {component_display_name}. Error: {str(e)}")
                            logs['ITComponent'].append({
                                "Action": "Creation Failed",
                                "FactSheet Type": "ITComponent",
                                "Name": component['name'],
                                "Error": str(e)
                            })

                # Identify and archive IT Components that are no longer present in new data
                for existing_component in leanix_it_components:
                    if user_cancel:
                        print("User canceled operation before starting. Saving logs up to this point...")
                        save_factsheet_logs_to_excel(logs)
                        return excel_file_path
                    component_display_name = existing_component['displayName']
                    component_parts = component_display_name.rsplit(' ', 1)

                    if len(component_parts) == 2:
                        component_name, component_release = component_parts
                    else:
                        component_name = component_parts[0]
                        component_release = ''

                    if component_display_name not in new_it_components:
                        if any(tag['name'] == tag_name for tag in existing_component['tags']) and \
                                existing_component[
                                    'category'].lower() == 'software':
                            if archive_it_component(existing_component['id'], component_display_name):
                                logs['ITComponent'].append({
                                    "Action": "Deleted",
                                    "FactSheet Type": "ITComponent",
                                    "Name": component_display_name,
                                    "ID": existing_component['id']

                                })
                                deleted_factsheets.append({
                                    "Name": component_display_name,
                                    "FactSheet Type": "ITComponent",
                                    "ID": existing_component['id']
                                })
                                log_and_save('ITComponent', {
                                    'Action': 'Deleted',
                                    "FactSheet Type": "ITComponent",
                                    "Name": component_display_name,
                                    "ID": existing_component['id']
                                })

                # IT Components
                component_query = """
                                            {
                                              allFactSheets(factSheetType: ITComponent) {
                                                edges {
                                                  node {
                                                    id
                                                    displayName
                                                    description
                                                    ... on ITComponent {
                                                          release
                                                          name
                                                    }
                                                    tags {
                                                        name
                                                    }
                                                    category
                                                  }
                                                }
                                              }
                                            }
                                            """
                response3 = requests.post(url=request_url, headers=headers, json={"query": component_query})
                response3.raise_for_status()
                leanix_it_components = [node['node'] for node in response3.json()['data']['allFactSheets']['edges']]

                # Generate display names for IT Components
                it_components_display_names = {f"{comp['name']} {comp['release']}": comp for comp in
                                               leanix_it_components}

                # Retrieve all applications again after creation
                applications_response = requests.post(url=request_url, headers=headers,
                                                      json={"query": application_query})
                applications_response.raise_for_status()
                applications = {app['node']['displayName']: app['node']['id'] for app in
                                applications_response.json()['data']['allFactSheets']['edges']}
                components_response = requests.post(url=request_url, headers=headers,
                                                    json={"query": component_query})
                components_response.raise_for_status()
                components = {component['node']['name']: component['node']['id'] for component in
                              components_response.json()['data']['allFactSheets']['edges']}

                # existing_relationships_query_components = """
                #                     {
                #                       allFactSheets(factSheetType: Application) {
                #                         edges {
                #                           node {
                #                             id
                #                             name
                #                             ... on Application {
                #                               relApplicationToITComponent {
                #                                 edges {
                #                                   node {
                #                                     id
                #                                     type
                #                                     factSheet {
                #                                       id
                #                                       name
                #                                     }
                #                                   }
                #                                 }
                #                               }
                #                             }
                #                           }
                #                         }
                #                       }
                #                     }
                #                     """
                #
                # response12 = requests.post(url=request_url, headers=headers,
                #                            json={"query": existing_relationships_query_components})
                # response12.raise_for_status()
                # existing_relationships_components = response12.json()['data']['allFactSheets']['edges']
                #
                # existing_relationships_components_set = set()
                # for app in existing_relationships_components:
                #     if user_cancel:
                #         print("User canceled operation before starting. Saving logs up to this point...")
                #         save_factsheet_logs_to_excel(logs)
                #         return excel_file_path
                #     app_id = app['node']['id']
                #     for relationship in app['node']['relApplicationToITComponent']['edges']:
                #         component_id = relationship['node']['factSheet']['id']
                #         existing_relationships_components_set.add((app_id, component_id))
                #
                # relationship_data_components = []
                #
                # for component in components_data:
                #     if user_cancel:
                #         print("User canceled operation before starting. Saving logs up to this point...")
                #         save_factsheet_logs_to_excel(logs)
                #         return excel_file_path
                #     app_name = f"{component['sid']} {component['product_version']}"
                #     component_name = component['name']
                #
                #     if app_name in applications and component_name in components:
                #         app_id = applications[app_name]
                #         component_id = components[component_name]
                #
                #         if (app_id, component_id) not in existing_relationships_components_set:
                #             relationship_data_components.append({
                #                 "Application ID": app_id,
                #                 "Application Name": app_name,
                #                 "IT Component ID": component_id,
                #                 "IT Component Name": component_name
                #             })
                #
                # relationship_df_components = pd.DataFrame(relationship_data_components)
                # relationship_df_components = relationship_df_components.drop_duplicates()
                #
                # for _, relationship in relationship_df_components.iterrows():
                #     if user_cancel:
                #         print("User canceled operation before starting. Saving logs up to this point...")
                #         save_factsheet_logs_to_excel(logs)
                #         return excel_file_path
                #     app_id = relationship["Application ID"]
                #     component_id = relationship["IT Component ID"]
                #
                #     relationship_mutation = """
                #                         mutation {
                #                             updateFactSheet(id: "%s", patches: [{op: add, path: "/relApplicationToITComponent/new_1", value: "%s"}]) {
                #                                 factSheet {
                #                                     id
                #                                     name
                #                                     ... on Application {
                #                                         relApplicationToITComponent {
                #                                             edges {
                #                                                 node {
                #                                                     id
                #                                                 }
                #                                             }
                #                                         }
                #                                     }
                #                                 }
                #                             }
                #                         }
                #                         """ % (app_id, component_id)
                #
                #     relation_response = requests.post(url=request_url, headers=headers,
                #                                       data=json.dumps({"query": relationship_mutation}))
                #     relation_response.raise_for_status()
                #
                #     print(
                #         f"Created relationship between Application factsheet '{relationship['Application Name']}' and IT Component factsheet '{relationship['IT Component Name']}'")
                #
                # # Technical Stack Functions

                # # ------------------------------------------ Technical Stack --------------------------------------------------
                # Technical Stack Functions
                def check_technical_stack_exists():
                    technical_stack_query = """
                                                    {
                                                      allFactSheets(factSheetType: TechnicalStack) {
                                                        edges {
                                                          node {
                                                            id
                                                            name
                                                          }
                                                        }
                                                      }
                                                    }
                                                    """

                    response6 = requests.post(url=request_url, headers=headers,
                                              json={"query": technical_stack_query})
                    response6.raise_for_status()
                    technical_stacks = {tech_stack['node']['name']: tech_stack['node']['id'] for tech_stack in
                                        response6.json()['data']['allFactSheets']['edges']}
                    return technical_stacks

                def create_technical_stack(tech_stack_name2):
                    mutation1 = """
                                                    mutation ($input: BaseFactSheetInput!, $patches: [Patch]!) {
                                                        createFactSheet(input: $input, patches: $patches) {
                                                            factSheet {
                                                                id
                                                                name
                                                                type
                                                                tags {
                                                                    id
                                                                    name
                                                                }
                                                            }
                                                        }
                                                    }
                                                    """
                    variables1 = {
                        "input": {
                            "name": tech_stack_name2,
                            "type": "TechnicalStack"
                        },
                        "patches": [
                            {
                                "op": "add",
                                "path": "/tags",
                                "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'
                            }
                        ],
                        "tagName": tag_name
                    }

                    data1 = {"query": mutation1, "variables": variables1}

                    response7 = requests.post(url=request_url, headers=headers, data=json.dumps(data1))
                    response7.raise_for_status()
                    tsid = response7.json()
                    ts_id = tsid['data']['createFactSheet']['factSheet']['id']
                    log_and_save('TechnicalStack', {
                        'Action': 'Created',
                        "FactSheet Type": "TechnicalStack",
                        "Name": tech_stack_name2,
                        "ID": ts_id
                    })
                    logs['TechnicalStack'].append({
                        "Action": "Created",
                        "FactSheet Type": "TechnicalStack",
                        "Name": tech_stack_name2,
                        "ID": ts_id
                    })
                    created_factsheets.append({
                        "Name": tech_stack_name2,
                        "FactSheet Type": "TechnicalStack",
                        "ID": ts_id
                    })
                    print(f"Technical Stack factsheet {tech_stack_name2} created")

                existing_technical_stacks = check_technical_stack_exists()
                components_categories = set(component['category'] for component in components_data)
                missing_technical_stacks = [category for category in components_categories if
                                            category not in existing_technical_stacks]
                logs['TechnicalStack'].extend(
                    [{"Action": "Created", "FactSheet Type": "TechnicalStack", "Name": stack} for stack in
                     missing_technical_stacks])

                for tech_stack_name in missing_technical_stacks:
                    if user_cancel:
                        log_and_save('TechnicalStack',
                                     {'Action': 'Cancelled', 'Message': 'User canceled operation'})
                        return excel_file_path
                    create_technical_stack(tech_stack_name)

                technical_stacks = check_technical_stack_exists()
                response9 = requests.post(url=request_url, headers=headers, json={"query": component_query})
                response9.raise_for_status()
                it_components = {it_component['node']['name']: it_component['node']['id'] for it_component in
                                 response9.json()['data']['allFactSheets']['edges']}

                # Fetch existing Technical Stacks from LeanIX
                technical_stack_query = """
                                                {
                                                  allFactSheets(factSheetType: TechnicalStack) {
                                                    edges {
                                                      node {
                                                        id
                                                        name
                                                        tags {
                                                            name
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                                """
                response6 = requests.post(url=request_url, headers=headers, json={"query": technical_stack_query})
                response6.raise_for_status()
                leanix_technical_stacks = [node['node'] for node in
                                           response6.json()['data']['allFactSheets']['edges']]

                # Compare and Identify Missing Technical Stacks
                new_technical_stack_names = list(set(component['category'] for component in components_data))

                # Identify Technical Stacks to delete
                technical_stacks_to_delete = [
                    tech_stack for tech_stack in leanix_technical_stacks
                    if tech_stack['name'] not in new_technical_stack_names
                       and any(tag['name'] == tag_name for tag in tech_stack['tags'])

                ]

                # Function to delete Technical Stack
                def delete_technical_stack(tech_stack_id, name):
                    mutation = """
                                                    mutation ($id: ID!) {
                                                        updateFactSheet(id: $id, comment: "Irrelevant Technical Stack", patches: [
                                                            {
                                                                op: add,
                                                                path: "/status",
                                                                value: "ARCHIVED"
                                                            }
                                                        ]) {
                                                            factSheet {
                                                                id
                                                                status
                                                            }
                                                        }
                                                    }
                                                    """
                    variables = {"id": tech_stack_id}
                    data = {"query": mutation, "variables": variables}
                    response = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                    response.raise_for_status()
                    print(f"Technical Stack factsheet {name} deleted")

                # Delete Technical Stacks that are not in the new data and tagged with "LA System"
                for tech_stack in technical_stacks_to_delete:
                    if user_cancel:
                        log_and_save('TechnicalStack',
                                     {'Action': 'Cancelled', 'Message': 'User canceled operation'})
                        return excel_file_path
                    tech_stack_id = tech_stack['id']
                    name = tech_stack['name']
                    delete_technical_stack(tech_stack_id, name)
                    logs['TechnicalStack'].append({
                        "Action": "Deleted",
                        "FactSheet Type": "TechnicalStack",
                        "Name": tech_stack['name']
                    })
                    deleted_factsheets.append({
                        "Name": tech_stack['name'],
                        "FactSheet Type": "TechnicalStack"
                    })
                    log_and_save('TechnicalStack', {
                        'Action': 'Deleted',
                        "FactSheet Type": "TechnicalStack",
                        "Name": tech_stack['name'],
                        "ID": tech_stack_id
                    })

                # # Retrieve existing relationships
                # existing_relationships_query = """
                #                     {
                #                       allFactSheets(factSheetType: ITComponent) {
                #                         edges {
                #                           node {
                #                             id
                #                             name
                #                             ... on ITComponent {
                #                               relITComponentToTechnologyStack {
                #                                 edges {
                #                                   node {
                #                                     id
                #                                     type
                #                                     factSheet {
                #                                       id
                #                                       name
                #                                     }
                #                                   }
                #                                 }
                #                               }
                #                             }
                #                           }
                #                         }
                #                       }
                #                     }
                #                     """
                #
                # response10 = requests.post(url=request_url, headers=headers,
                #                            json={"query": existing_relationships_query})
                # response10.raise_for_status()
                # existing_relationships = response10.json()['data']['allFactSheets']['edges']
                #
                # existing_relationships_set = set()
                # for it_component in existing_relationships:
                #     component_id = it_component['node']['id']
                #     for relationship in it_component['node']['relITComponentToTechnologyStack']['edges']:
                #         technical_stack_id = relationship['node']['factSheet']['id']
                #         existing_relationships_set.add((component_id, technical_stack_id))
                #
                # relationship_data = []
                #
                # # Prepare unique relationship data before making the API calls
                # for component in components_data:
                #     component_name = component['name']
                #     component_category = component['category']
                #
                #     if component_name in it_components and component_category in technical_stacks:
                #         component_id = it_components[component_name]
                #         technical_stack_id = technical_stacks[component_category]
                #
                #         if (component_id, technical_stack_id) not in existing_relationships_set:
                #             relationship_data.append({
                #                 "IT Component ID": component_id,
                #                 "IT Component Name": component_name,
                #                 "Technical Stack ID": technical_stack_id,
                #                 "Technical Stack Name": component_category
                #             })
                #
                # # Convert relationship data to a DataFrame
                # relationship_df = pd.DataFrame(relationship_data)
                #
                # # Drop duplicates to ensure unique relationships
                # relationship_df = relationship_df.drop_duplicates()
                #
                # # Proceed with the API calls to create the unique relationships
                # for _, relationship in relationship_df.iterrows():
                #     component_id = relationship["IT Component ID"]
                #     technical_stack_id = relationship["Technical Stack ID"]
                #
                #     relationship_mutation = """
                #                          mutation {
                #                     updateFactSheet(
                #                       id: "%s"
                #                       patches: [{op: add, path: "/relITComponentToTechnologyStack/new_1", value: "%s"}]
                #                     ) {
                #                       factSheet {
                #                         id
                #                         name
                #                         ... on ITComponent {
                #                           relITComponentToTechnologyStack {
                #                             edges {
                #                               node {
                #                                 id
                #                               }
                #                             }
                #                           }
                #                         }
                #                       }
                #                     }
                #                     }
                #                         """ % (component_id, technical_stack_id)
                #
                #     relation_response = requests.post(url=request_url, headers=headers,
                #                                       data=json.dumps({"query": relationship_mutation}))
                #     relation_response.raise_for_status()
                #
                #     print(
                #         f"Created relationship between IT Component factsheet '{relationship['IT Component Name']}' and "
                #         f"Technical Stack factsheet'{relationship['Technical Stack Name']}'")

                # # ============================================= Hosts ================================================================

                # Retrieve all applications and IT components again after creation
                applications_response = requests.post(url=request_url, headers=headers,
                                                      json={"query": application_query})
                applications_response.raise_for_status()
                applications = {app['node']['displayName']: app['node']['id'] for app in
                                applications_response.json()['data']['allFactSheets']['edges']}

                components_response = requests.post(url=request_url, headers=headers,
                                                    json={"query": component_query})
                components_response.raise_for_status()
                components = {component['node']['name']: component['node']['id'] for component in
                              components_response.json()['data']['allFactSheets']['edges']}

                # Get existing IT Components
                existing_it_components = [component['node']['name'] for component in
                                          components_response.json()['data']['allFactSheets']['edges']]
                # A set to track the names of IT Components that have been created
                created_it_components = set()

                for host in hosts_data:
                    # Check if user canceled
                    if user_cancel:
                        log_and_save('ITComponent', {'Action': 'Cancelled', 'Message': 'User canceled operation'})
                        return excel_file_path
                    host_name = host['name']
                    ip_address = host['ip_address']
                    category = host['category']

                    if host_name in existing_it_components or host_name in created_it_components:
                        continue  # Skip this host as it already exists or was just created

                    # Check if the IT component already exists
                    if host_name not in existing_it_components:

                        # Define your GraphQL mutation to create an IT Component, including an alias for the IP address

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
                                "name": host_name,
                                "type": "ITComponent"
                            },
                            "patches": [
                                {
                                    "op": "add",
                                    "path": "/category",
                                    "value": "hardware"
                                },
                                {
                                    "op": "add",
                                    "path": "/tags",
                                    "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'
                                },
                                {
                                    "op": "add",
                                    "path": "/alias",
                                    "value": ip_address
                                },
                                {
                                    "op": "add",
                                    "path": "/description",
                                    "value": category
                                }
                            ],
                            "tagName": tag_name
                        }

                        data = {"query": mutation, "variables": variables}

                        response10 = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                        response10.raise_for_status()
                        it2 = response10.json()
                        print(f"IT Component factsheet {host_name} created")

                        # Check if the response contains the expected data and not just an error
                        if it2 and 'data' in it2 and 'createFactSheet' in it2['data'] and 'factSheet' in \
                                it2['data'][
                                    'createFactSheet']:
                            it_2 = it2['data']['createFactSheet']['factSheet']['id']
                            # Log the creation action
                            logs['ITComponent'].append({
                                "Action": "Created",
                                "FactSheet Type": "ITComponent",
                                "Name": host_name,
                                'ID': it_2
                            })
                            created_factsheets.append({
                                "Name": host_name,
                                "FactSheet Type": "ITComponent",
                                'ID': it_2
                            })
                            log_and_save("ITComponent", {
                                "Action": "Created",
                                "FactSheet Type": "ITComponent",
                                "Name": host_name,
                                'ID': it_2
                            })
                            # Add this IT Component name to the created set to avoid future duplicates
                            created_it_components.add(host_name)
                            print(f"IT Component factsheet {host_name} Created")
                        else:
                            # Handle the error case
                            if 'errors' in it2:
                                print(f"Error creating IT Component for host {host_name}: {it2['errors']}")
                            else:
                                print(
                                    f"Unexpected response structure while creating IT Component for host {host_name}: {it2}")
                            continue  # Move on to the next host

                # Function to update IT Component alias
                def update_it_component_alias(component_id, alias, category):
                    mutation = """
                                                        mutation ($id: ID!, $patches: [Patch]!) {
                                                            updateFactSheet(id: $id, patches: $patches) {
                                                                factSheet {
                                                                    id
                                                                    name
                                                                    ... on ITComponent {
                                                                        alias
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        """
                    variables = {
                        "id": component_id,
                        "patches": [
                            {
                                "op": "replace",
                                "path": "/alias",
                                "value": alias
                            },
                            {
                                "op": "replace",
                                "path": "/description",
                                "value": category
                            }
                        ]
                    }
                    data = {"query": mutation, "variables": variables}
                    response = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                    response.raise_for_status()

                # Fetch existing IT Components from LeanIX, including tags
                component_query_with_tags = """
                                                        {
                                                          allFactSheets(factSheetType: ITComponent) {
                                                            edges {
                                                              node {
                                                                id
                                                                name
                                                                category
                                                                tags {
                                                                  name
                                                                }
                                                              }
                                                            }
                                                          }
                                                        }
                                                        """
                response_with_tags = requests.post(url=request_url, headers=headers,
                                                   json={"query": component_query_with_tags})
                response_with_tags.raise_for_status()
                leanix_it_components_with_tags = [node['node'] for node in
                                                  response_with_tags.json()['data']['allFactSheets']['edges']]

                # Identify existing IT components' names
                existing_it_components_names = [
                    component['name'] for component in leanix_it_components_with_tags
                    if component['category'] == 'hardware'
                ]

                # Identify new IT components' names from the XML data
                new_it_components_names = [
                    host['name'] for host in hosts_data
                ]

                # Identify IT components to delete
                components_to_delete = [
                    component for component in leanix_it_components_with_tags
                    if component['name'] not in new_it_components_names
                       and 'tags' in component and any(tag['name'] == tag_name for tag in component['tags'])
                       and component['category'] == 'hardware'
                ]

                # Function to delete IT Component
                def delete_it_component(component_id, name):
                    mutation = """
                                                            mutation ($id: ID!) {
                                                                updateFactSheet(id: $id, comment: "Irrelevant IT Component", patches: [
                                                                    {
                                                                        op: add,
                                                                        path: "/status",
                                                                        value: "ARCHIVED"
                                                                    }
                                                                ]) {
                                                                    factSheet {
                                                                        id
                                                                        status
                                                                    }
                                                                }
                                                            }
                                                            """
                    variables = {"id": component_id}
                    data = {"query": mutation, "variables": variables}
                    response = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                    response.raise_for_status()
                    print(f"IT Component factsheet {name} deleted")

                # Delete IT Components that are not in the new data, tagged with "LA System", and in the "hardware" category
                for component in components_to_delete:
                    component_id = component['id']
                    name = component['name']
                    delete_it_component(component_id, name)
                    logs['ITComponent'].append({
                        "Action": "Deleted",
                        "FactSheet Type": "ITComponent",
                        "Name": name,
                        "ID": component_id
                    })
                    deleted_factsheets.append({
                        "Action": "Deleted",
                        "Name": name,
                        "FactSheet Type": "ITComponent",
                        "ID": component_id
                    })

                # # Fetch existing relationships for hosts
                # existing_relationships_query_hosts = """
                #                                 {
                #                                   allFactSheets(factSheetType: Application) {
                #                                     edges {
                #                                       node {
                #                                         id
                #                                         name
                #                                         ... on Application {
                #                                           relApplicationToITComponent {
                #                                             edges {
                #                                               node {
                #                                                 id
                #                                                 type
                #                                                 factSheet {
                #                                                   id
                #                                                   name
                #                                                 }
                #                                               }
                #                                             }
                #                                           }
                #                                         }
                #                                       }
                #                                     }
                #                                   }
                #                                 }
                #                                 """
                #
                # response11 = requests.post(url=request_url, headers=headers,
                #                            json={"query": existing_relationships_query_hosts})
                # response11.raise_for_status()
                # existing_relationships_hosts = response11.json()['data']['allFactSheets']['edges']
                #
                # existing_relationships_hosts_set = set()
                # for app in existing_relationships_hosts:
                #     app_id = app['node']['id']
                #     for relationship in app['node']['relApplicationToITComponent']['edges']:
                #         component_id = relationship['node']['factSheet']['id']
                #         existing_relationships_hosts_set.add((app_id, component_id))
                #
                # # Prepare unique relationship data before making the API calls
                # relationship_data_hosts = []
                #
                # for host in hosts_data:
                #     app_name = f"{host['sid']} {host['product_version']}"
                #     host_name = host['name']
                #
                #     if app_name in applications and host_name in it_components:
                #         app_id = applications[app_name]
                #         component_id = it_components[host_name]
                #
                #         if (app_id, component_id) not in existing_relationships_hosts_set:
                #             relationship_data_hosts.append({
                #                 "Application ID": app_id,
                #                 "Application Name": app_name,
                #                 "IT Component ID": component_id,
                #                 "IT Component Name": host_name
                #             })
                #
                # # Convert relationship data to a DataFrame
                # relationship_df_hosts = pd.DataFrame(relationship_data_hosts)
                #
                # # Drop duplicates to ensure unique relationships
                # relationship_df_hosts = relationship_df_hosts.drop_duplicates()
                #
                # response_data = response3.json()
                # if 'errors' in response_data:
                #     print("Error in response:", response_data['errors'])
                #     leanix_it_components = []
                # else:
                #     leanix_it_components_data = response_data.get('data', {}).get('allFactSheets', {}).get('edges',
                #                                                                                            [])
                #     if leanix_it_components_data is None:
                #         leanix_it_components_data = []
                #     leanix_it_components = [node['node'] for node in leanix_it_components_data]
                #
                # # Prepare a dictionary of existing IT components for quick lookup
                # existing_it_components_dict = {comp['name']: comp for comp in leanix_it_components}
                #
                # for host in hosts_data:
                #     host_name = host['name']
                #     ip_address = host['ip_address']
                #     category = host['category']
                #
                #     if host_name in existing_it_components_dict:
                #         existing_component = existing_it_components_dict[host_name]
                #         existing_alias = existing_component.get('alias', '')
                #         existing_description = existing_component.get('description', '')
                #
                #         # Check if alias (IP address) or description (category) has changed
                #         if existing_alias != ip_address or existing_description != category:
                #             update_it_component_alias(existing_component['id'], ip_address, category)
                #             logs['ITComponent'].append({
                #                 "Action": "Updated",
                #                 "FactSheet Type": "ITComponent",
                #                 "Name": host_name,
                #                 "Alias": ip_address
                #             })
                #             updated_factsheets.append({
                #                 "Action": "Updated",
                #                 "FactSheet Type": "ITComponent",
                #                 "Name": host_name,
                #                 "Alias": ip_address
                #             })
                #             print(f"IT component factsheet {host_name} updated")
                #
                # # Proceed with the API calls to create the unique relationships
                # for _, relationship in relationship_df_hosts.iterrows():
                #     app_id = relationship["Application ID"]
                #     component_id = relationship["IT Component ID"]
                #
                #     relationship_mutation = """
                #                                     mutation {
                #                                         updateFactSheet(id: "%s", patches: [{op: add, path: "/relApplicationToITComponent/new_1", value: "%s"}]) {
                #                                             factSheet {
                #                                                 id
                #                                                 name
                #                                                 ... on Application {
                #                                                     relApplicationToITComponent {
                #                                                         edges {
                #                                                             node {
                #                                                                 id
                #                                                             }
                #                                                         }
                #                                                     }
                #                                                 }
                #                                             }
                #                                         }
                #                                     }
                #                                     """ % (app_id, component_id)
                #
                #     relation_response = requests.post(url=request_url, headers=headers,
                #                                       data=json.dumps({"query": relationship_mutation}))
                #     relation_response.raise_for_status()
                #
                #     print(
                #         f"Created relationship between Application factsheet '{relationship['Application Name']}' and IT Component factsheet '{relationship['IT Component Name']}'")
                #
                # # ============================================= Clients ================================================================

                def fetch_existing_applications(request_url, headers):
                    graphql_query = """
                                                {
                                                  allFactSheets(factSheetType: Application) {
                                                    edges {
                                                      node {
                                                        id
                                                        name
                                                        description
                                                      }
                                                    }
                                                  }
                                                }
                                                """
                    response = requests.post(url=request_url, headers=headers, json={"query": graphql_query})
                    response.raise_for_status()
                    applications = response.json()['data']['allFactSheets']['edges']
                    return {normalize_name(app['node']['name']): app['node'] for app in applications}

                # Fetch existing applications
                existing_applications_dict = fetch_existing_applications(request_url, headers)

                application_level2 = []

                # Function to create application
                def create_application(name, description):
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
                            "name": name,
                            "type": "Application"
                        },
                        "patches": [
                            {"op": "add", "path": "/tags",
                             "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'},
                            {"op": "add", "path": "/description", "value": description}
                        ],
                        "tagName": tag_name
                    }
                    data = {"query": mutation, "variables": variables}
                    response = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                    response.raise_for_status()
                    response_data = response.json()
                    return response_data['data']['createFactSheet']['factSheet']['id']

                # Function to update application description
                def update_application_description(application_id, description):
                    mutation = """
                                                mutation ($id: ID!, $patches: [Patch]!) {
                                                    updateFactSheet(id: $id, patches: $patches) {
                                                        factSheet {
                                                            id
                                                            name
                                                            description
                                                        }
                                                    }
                                                }
                                                """
                    variables = {
                        "id": application_id,
                        "patches": [{"op": "replace", "path": "/description", "value": description}]
                    }
                    data = {"query": mutation, "variables": variables}
                    response = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                    response.raise_for_status()

                # Process clients data
                for client in clients_data:
                    it_component_name = f"{client['sid']}-{client['client_id']} {client['product_version']}"
                    new_description = client['description']
                    norm_name = normalize_name(it_component_name)

                    if norm_name in existing_applications_dict:
                        app_id = existing_applications_dict[norm_name]['id']
                        current_description = existing_applications_dict[norm_name]['description']

                        if new_description != current_description:
                            update_application_description(app_id, new_description)
                            logs['Application'].append({
                                "Action": "Updated",
                                "FactSheet Type": "Application",
                                "Name": it_component_name,
                                "Description": new_description,
                                "ID": app_id
                            })

                            updated_factsheets.append({
                                "Name": it_component_name,
                                "Description": new_description,
                                "FactSheet Type": "Application",
                                "ID": app_id
                            })
                            print(
                                f"Application factsheet {it_component_name} with description {new_description} updated")

                        application_level2.append(
                            {"name": it_component_name, "id": app_id, "description": new_description})
                    else:
                        app_id = create_application(it_component_name, new_description)
                        application_level2.append(
                            {"name": it_component_name, "id": app_id, "description": new_description})
                        logs['Application'].append({
                            "Action": "Created",
                            "FactSheet Type": "Application",
                            "Name": it_component_name,
                            "ID": app_id
                        })
                        created_factsheets.append({
                            "Action": "Created",
                            "Name": it_component_name,
                            "FactSheet Type": "Application",
                            "ID": app_id
                        })
                        print(f"Application factsheet {it_component_name} created")

                # # Fetch existing relationships for clients
                # existing_relationships_query_clients = """
                #                     {
                #                       allFactSheets(factSheetType: Application) {
                #                         edges {
                #                           node {
                #                             id
                #                             name
                #                             ... on Application {
                #                               relToChild {
                #                                 edges {
                #                                   node {
                #                                     id
                #                                     type
                #                                     factSheet {
                #                                       id
                #                                       name
                #                                     }
                #                                   }
                #                                 }
                #                               }
                #                             }
                #                           }
                #                         }
                #                       }
                #                     }
                #                     """
                #
                # response14 = requests.post(url=request_url, headers=headers,
                #                            json={"query": existing_relationships_query_clients})
                # response14.raise_for_status()
                # existing_relationships_clients = response14.json()['data']['allFactSheets']['edges']
                #
                # existing_relationships_clients_set = set()
                # for app in existing_relationships_clients:
                #     parent_id = app['node']['id']
                #     for relationship in app['node']['relToChild']['edges']:
                #         child_id = relationship['node']['factSheet']['id']
                #         existing_relationships_clients_set.add((parent_id, child_id))
                #
                # # Prepare unique relationship data before making the API calls
                # relationship_data_clients = []
                #
                # for system in systems_data:
                #     parent_name = f"{system['sid']} {system['product_version']}"
                #     parent_id = applications.get(parent_name)
                #     if parent_id:  # Ensure the parent application exists
                #         for client in clients_data:
                #             if system['sid'] == client['sid']:  # Ensure the client belongs to the same system
                #                 child_name = f"{client['sid']}-{client['client_id']} {client['product_version']}"
                #                 child_id = applications.get(child_name)
                #                 if child_id:  # Ensure the child application exists
                #                     if (parent_id, child_id) not in existing_relationships_clients_set:
                #                         relationship_data_clients.append({
                #                             "Parent Application ID": parent_id,
                #                             "Parent Application Name": parent_name,
                #                             "Child Application ID": child_id,
                #                             "Child Application Name": child_name
                #                         })
                #
                # # Convert relationship data to a DataFrame
                # relationship_df_clients = pd.DataFrame(relationship_data_clients)
                #
                # # Drop duplicates to ensure unique relationships
                # relationship_df_clients = relationship_df_clients.drop_duplicates()

                # ============================================= Modules =============================================================

                # List to hold module-level application factsheet details
                module_level_applications = []

                # Fetch existing factsheets
                def query_factsheet():
                    query = '''
                                                {
                                              allFactSheets(factSheetType: Application) {
                                                edges {
                                                  node {
                                                  id
                                                  name
                                                displayName
                                                  }
                                                }
                                              }
                                            }
                                                '''
                    response16 = requests.post(request_url, headers=headers, json={'query': query})
                    response16.raise_for_status()
                    return response16.json()

                existing_factsheets_response = query_factsheet()
                existing_factsheets = {node['node']['name']: node['node'] for node in
                                       existing_factsheets_response['data']['allFactSheets']['edges']}

                existing_names = {node['node']['name'] for node in
                                  existing_factsheets_response['data']['allFactSheets']['edges']}

                # Function to create a new factsheet
                def create_factsheet(factsheet_name, description):
                    mutation5 = """
                                                mutation ($input: BaseFactSheetInput!, $patches: [Patch]!) {
                                                  createFactSheet(input: $input, patches: $patches) {
                                                    factSheet {
                                                      name
                                                      type
                                                      id
                                                      description
                                                      tags {
                                                        id
                                                        name
                                                      }
                                                    }
                                                  }
                                                }
                                                                            """
                    variables5 = {
                        "input": {
                            "name": factsheet_name,
                            "type": "Application"
                        },
                        "patches": [
                            {
                                "op": "add",
                                "path": "/tags",
                                "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'
                            },
                            {
                                "op": "add",
                                "path": "/description",
                                "value": description
                            }
                        ],
                        "tagName": tag_name
                    }

                    data3 = {"query": mutation5, "variables": variables5}

                    response17 = requests.post(url=request_url, headers=headers, data=json.dumps(data3))
                    response17.raise_for_status()
                    response_json = response17.json()

                    if 'data' in response_json and 'createFactSheet' in response_json['data']:
                        return response_json['data']['createFactSheet']['factSheet']
                    else:
                        raise Exception("Unexpected response format: {}".format(response_json))

                # Iterate over module data to create or update factsheets
                for system in modules_data:
                    factsheet_name = system['belongs_to']
                    sub_module = system['acronym'] + "(" + system['sid'] + "-" + system['Client_id'] + ")"
                    description = system.get('description', '')

                    if sub_module not in existing_names:
                        result3 = create_factsheet(sub_module, description)
                        module_level_applications.append({
                            "name": sub_module,
                            "description": description,
                            "ID": result3['id']
                        })
                        logs['Application'].append({
                            "Action": "Created",
                            "FactSheet Type": "Application",
                            "Name": sub_module,
                            "ID": result3['id']
                        })
                        created_factsheets.append({
                            "Action": "Created",
                            "Name": sub_module,
                            "FactSheet Type": "Application",
                            "ID": result3['id']
                        })
                        print(f"Application factsheet {sub_module} created")
                    else:
                        module_level_applications.append({
                            "name": sub_module,
                            "description": description
                        })

                # Save module-level application factsheet details to an Excel file
                application_level3 = pd.DataFrame(module_level_applications)

                # Combine application levels into a single DataFrame
                application_level1_df = pd.DataFrame(application_level1)

                application_level2_df = pd.DataFrame(application_level2)
                application_level3_df = pd.DataFrame(application_level3)

                combined_applications_df = pd.concat(
                    [application_level1_df, application_level2_df, application_level3_df]).reset_index(
                    drop=True)

                # Fetch existing application factsheets with the "LA System" tag
                existing_application_query = """
                                            {
                                              allFactSheets(factSheetType: Application) {
                                                edges {
                                                  node {
                                                    id
                                                    displayName
                                                    name
                                                    tags {
                                                      name
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                            """

                response = requests.post(url=request_url, headers=headers,
                                         json={"query": existing_application_query})
                response.raise_for_status()
                existing_applications = response.json()['data']['allFactSheets']['edges']

                # Filter applications with the "LA System" tag
                la_system_applications = [
                    app['node'] for app in existing_applications
                    if any(tag['name'] == tag_name for tag in app['node']['tags'])
                ]

                # Identify applications to delete (those with the "LA System" tag but not in the combined DataFrame)
                combined_application_names = combined_applications_df['name'].tolist()
                applications_to_delete = [
                    app for app in la_system_applications
                    if app['name'] not in combined_application_names
                ]

                # Function to delete application factsheet
                def delete_application(app_id, name):
                    mutation = """
                                                            mutation ($id: ID!) {
                                                                updateFactSheet(id: $id, comment: "Irrelevant Application", patches: [
                                                                    {
                                                                        op: add,
                                                                        path: "/status",
                                                                        value: "ARCHIVED"
                                                                    }
                                                                ]) {
                                                                    factSheet {
                                                                        id
                                                                        status
                                                                    }
                                                                }
                                                            }
                                                            """
                    variables = {"id": app_id}
                    data = {"query": mutation, "variables": variables}
                    response = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                    response.raise_for_status()
                    print(f"Application factsheet {name} deleted")

                # Delete applications that are not in the combined DataFrame
                for app in applications_to_delete:
                    app_id = app['id']
                    name = app['name']
                    delete_application(app_id, name)
                    logs['Application'].append({
                        "Action": "Deleted",
                        "FactSheet Type": "Application",
                        "Name": name,
                        "ID": app_id
                    })
                    deleted_factsheets.append({
                        "Action": "Deleted",
                        "Name": name,
                        "FactSheet Type": "Application",
                        "ID": app_id
                    })

                # Fetch existing relationships for modules
                application_query = """{
                                             allFactSheets(factSheetType: Application) {
                                               edges {
                                                 node {
                                                   id
                                                   displayName
                                                   name
                                                 }
                                               }
                                             }
                                           }"""

                response22 = requests.post(url=request_url, headers=headers, json={"query": application_query})
                response22.raise_for_status()
                data_frame = pd.json_normalize(response22.json())

                applications = {node['node']['name'].strip(): node['node']['id'] for node in
                                response22.json()['data']['allFactSheets']['edges']}

                # # Fetch existing relationships for modules
                # existing_relationships_query_modules = """
                #                            {
                #                              allFactSheets(factSheetType: Application) {
                #                                edges {
                #                                  node {
                #                                    id
                #                                    name
                #                                    ... on Application {
                #                                      relToChild {
                #                                        edges {
                #                                          node {
                #                                            id
                #                                            type
                #                                            factSheet {
                #                                              id
                #                                              name
                #                                            }
                #                                          }
                #                                        }
                #                                      }
                #                                    }
                #                                  }
                #                                }
                #                              }
                #                            }
                #                            """
                #
                # response16 = requests.post(url=request_url, headers=headers,
                #                            json={"query": existing_relationships_query_modules})
                # response16.raise_for_status()
                # existing_relationships_modules = response16.json()['data']['allFactSheets']['edges']
                #
                # existing_relationships_modules_set = set()
                # for app in existing_relationships_modules:
                #     parent_id = app['node']['id']
                #     for relationship in app['node']['relToChild']['edges']:
                #         child_id = relationship['node']['factSheet']['id']
                #         existing_relationships_modules_set.add((parent_id, child_id))
                #
                # # Prepare unique relationship data before making the API calls
                # relationship_data_modules = []
                #
                # for client in clients_data:
                #     parent_name = f"{client['sid']}-{client['client_id']} {client['product_version']}".strip()
                #     parent_id = applications.get(parent_name)
                #
                #     if parent_id:  # Ensure the parent application exists
                #         for module in modules_data:
                #             if module['sid'] == client['sid'] and module['Client_id'] == client['client_id']:
                #                 child_name = f"{module['acronym']}({module['sid']}-{module['Client_id']})".strip()
                #                 child_id = applications.get(child_name)
                #
                #                 if child_id:  # Ensure the child application exists
                #                     if (parent_id, child_id) not in existing_relationships_modules_set:
                #                         relationship_data_modules.append({
                #                             "Parent Application ID": parent_id,
                #                             "Parent Application Name": parent_name,
                #                             "Child Application ID": child_id,
                #                             "Child Application Name": child_name
                #                         })
                #
                # # Convert relationship data to a DataFrame
                # relationship_df_modules = pd.DataFrame(relationship_data_modules)
                #
                # # Drop duplicates to ensure unique relationships
                # relationship_df_modules = relationship_df_modules.drop_duplicates()
                #
                # # Proceed with the API calls to create the unique relationships
                # for _, relationship in relationship_df_modules.iterrows():
                #     parent_id = relationship["Parent Application ID"]
                #     child_id = relationship["Child Application ID"]
                #
                #     relationship_mutation = f'''
                #                                mutation {{
                #                                  updateFactSheet(id: "{parent_id}", patches: [
                #                                    {{
                #                                      op: add,
                #                                      path: "/relToChild/new_1",
                #                                      value: "{{\\"factSheetId\\":\\"{child_id}\\"}}"
                #                                    }}
                #                                  ]) {{
                #                                    factSheet {{
                #                                      id
                #                                      name
                #                                      ... on Application {{
                #                                        relToChild {{
                #                                          edges {{
                #                                            node {{
                #                                              id
                #                                            }}
                #                                          }}
                #                                        }}
                #                                      }}
                #                                    }}
                #                                  }}
                #                                }}
                #                                '''
                #     response17 = requests.post(url=request_url, headers=headers,
                #                                data=json.dumps({"query": relationship_mutation}))
                #     response17.raise_for_status()
                #
                #     print(
                #         f"Created relationship between Parent Application '{relationship['Parent Application Name']}' and Child Application '{relationship['Child Application Name']}'")
                #
                # # ======================================= Business Capability =========================================

                # Function to fetch existing Business Capability FactSheets
                def get_existing_business_capabilities():
                    query = """
                    {
                      allFactSheets(factSheetType: BusinessCapability) {
                        edges {
                          node {
                            id
                            name
                          }
                        }
                      }
                    }
                    """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    return {data['node']['name']: data['node']['id'] for data in
                            response.json()['data']['allFactSheets']['edges']}

                existing_business_capabilities = get_existing_business_capabilities()

                # Function to create a Business Capability FactSheet if it does not exist
                def create_business_capability(name):
                    mutation5 = """
                    mutation ($input: BaseFactSheetInput!, $patches: [Patch]!) {
                        createFactSheet(input: $input, patches: $patches) {
                            factSheet {
                                name
                                id
                                type
                                tags {
                                    id
                                    name
                                }
                            }
                        }
                    }
                    """
                    variables5 = {
                        "input": {
                            "name": name,
                            "type": "BusinessCapability"
                        },
                        "patches": [
                            {
                                "op": "add",
                                "path": "/tags",
                                "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'
                            }
                        ]
                    }

                    data3 = {"query": mutation5, "variables": variables5}

                    try:
                        response18 = requests.post(url=request_url, headers=headers, data=json.dumps(data3))
                        response18.raise_for_status()

                        response_json = response18.json()

                        # Check if 'data' and 'createFactSheet' are present in the response
                        factsheet_data = response_json.get('data', {}).get('createFactSheet')

                        if not factsheet_data or 'factSheet' not in factsheet_data:
                            logs['BusinessCapability'].append({
                                'Name': name,
                                'Action': 'Error',
                                'Message': 'Missing factSheet in API response'
                            })
                            return  # Skip this fact sheet

                        factsheet = factsheet_data['factSheet']
                        factsheet_id = factsheet.get('id')

                        if not factsheet_id:
                            print(f"Error: 'id' missing in factSheet for {name}. Skipping this fact sheet.")
                            logs['BusinessCapability'].append({
                                'Name': name,
                                'Action': 'Error',
                                'Message': "FactSheet created but 'id' missing"
                            })
                            return  # Skip if there's no 'id'

                        print(f"Business Capability FactSheet {name} Created with ID: {factsheet_id}")

                        # Log the creation action and store the ID
                        logs['BusinessCapability'].append({
                            'Name': name,
                            'Action': 'Created',
                            'FactSheet Type': 'BusinessCapability',
                            'ID': factsheet_id
                        })
                        log_and_save('BusinessCapability', {
                            'Name': name,
                            'Action': 'Created',
                            'FactSheet Type': 'BusinessCapability',
                            'ID': factsheet_id
                        })

                    except (requests.exceptions.RequestException, KeyError, TypeError) as e:
                        # Catching possible exceptions related to API errors or missing data
                        print(f"An error occurred while creating {name}: {str(e)}. Skipping this fact sheet.")
                        logs['BusinessCapability'].append({
                            'Name': name,
                            'Action': 'Error',
                            'Message': str(e)
                        })

                # Function to process modules data, collect unique names, and create Business Capabilities
                def process_modules_data(modules_data1, existing_business_capabilities1):
                    # Collect all business capability names that need to be created
                    business_capability_names = [module['name'] for module in modules_data1 if
                                                 module['name'] not in existing_business_capabilities1]

                    # Remove duplicates by converting the list to a set and then back to a list
                    unique_business_capability_names = list(set(business_capability_names))

                    # Iterate over the unique business capability names and create them
                    for name in unique_business_capability_names:
                        if user_cancel:
                            log_and_save('BusinessCapability',
                                         {'Action': 'Cancelled', 'Message': 'User canceled operation'})
                            return excel_file_path

                        create_business_capability(name)

                # Process modules data and log the creation
                process_modules_data(modules_data, existing_business_capabilities)

                def get_existing_business_capabilities():
                    query = """
                                                    {
                                                      allFactSheets(factSheetType: BusinessCapability) {
                                                        edges {
                                                          node {
                                                            id
                                                            name
                                                            tags {
                                                              name
                                                            }
                                                          }
                                                        }
                                                      }
                                                    }
                                                    """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    return {data['node']['name']: {'id': data['node']['id'], 'tags': data['node']['tags']} for data
                            in
                            response.json()['data']['allFactSheets']['edges']}

                existing_business_capabilities = get_existing_business_capabilities()

                # def process_modules_data(modules_data1, existing_business_capabilities1):
                #     for module in modules_data1:
                #         module_name = module['name']
                #         if module_name not in existing_business_capabilities1:
                #             create_business_capability(module_name)
                #
                #
                # process_modules_data(modules_data, existing_business_capabilities)

                # List of new business capability names from the XML data
                new_business_capability_names = [module['name'] for module in modules_data]

                # Identify Business Capabilities to delete
                la_system_tagged_bc = [bc for bc in existing_business_capabilities if
                                       any(tag['name'] == tag_name for tag in
                                           existing_business_capabilities[bc]['tags'])]
                bc_to_delete = [bc for bc in la_system_tagged_bc if bc not in new_business_capability_names]

                # Function to delete Business Capability
                def delete_business_capability(bc_id, name):
                    mutation = """
                                                    mutation ($id: ID!) {
                                                        updateFactSheet(id: $id, comment: "Irrelevant Business Capability", patches: [
                                                            {
                                                                op: add,
                                                                path: "/status",
                                                                value: "ARCHIVED"
                                                            }
                                                        ]) {
                                                            factSheet {
                                                                id
                                                                status
                                                            }
                                                        }
                                                    }
                                                    """
                    variables = {"id": bc_id}
                    data = {"query": mutation, "variables": variables}
                    response = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                    response.raise_for_status()
                    print(f"Business Capability factsheet {name} deleted")

                # Delete Business Capabilities that are not in the new data and tagged with "LA System"
                for bc in bc_to_delete:
                    bc_id = existing_business_capabilities[bc]['id']
                    delete_business_capability(bc_id, bc)
                    logs['BusinessCapability'].append({
                        "Action": "Deleted",
                        "FactSheet Type": "BusinessCapability",
                        "Name": bc,
                        "ID": bc_id
                    })
                    deleted_factsheets.append({
                        "Action": "Deleted",
                        "Name": bc,
                        "FactSheet Type": "BusinessCapability",
                        "ID": bc_id
                    })

                # Function to fetch fact sheets
                def fetch_fact_sheets(fact_sheet_type):
                    query = f"""
                                         {{
                                           allFactSheets(factSheetType: {fact_sheet_type}) {{
                                             edges {{
                                               node {{
                                                 id
                                                 name
                                               }}
                                             }}
                                           }}
                                         }}
                                         """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    return {node['node']['name']: node['node']['id'] for node in
                            response.json()['data']['allFactSheets']['edges']}

                # # Function to create a relationship and print details
                # def create_relationship(app_id2, app_name, biz_cap_id, biz_cap_name):
                #     mutation4 = f'''
                #                              mutation {{
                #                                updateFactSheet(id: "{biz_cap_id}", patches: [
                #                                  {{
                #                                    op: add,
                #                                    path: "/relBusinessCapabilityToApplication/new_1",
                #                                    value: "{{\\"factSheetId\\":\\"{app_id2}\\"}}"
                #                                  }}
                #                                ]) {{
                #                                  factSheet {{
                #                                    id
                #                                    name
                #                                    ... on BusinessCapability {{
                #                                      relBusinessCapabilityToApplication {{
                #                                        edges {{
                #                                          node {{
                #                                            id
                #                                          }}
                #                                        }}
                #                                      }}
                #                                    }}
                #                                  }}
                #                                }}
                #                              }}
                #                              '''
                #
                #     response = requests.post(url=request_url, headers=headers, json={"query": mutation4})
                #     response.raise_for_status()
                #     print(
                #         f"Relationship created between Application '{app_name}' and Business Capability '{biz_cap_name}'")
                #
                # # Fetch all applications and business capabilities
                # applications = fetch_fact_sheets("Application")
                # business_capabilities = fetch_fact_sheets("BusinessCapability")
                #
                # # Fetch existing relationships
                # existing_relationships_query_bc = """
                #                      {
                #                        allFactSheets(factSheetType: BusinessCapability) {
                #                          edges {
                #                            node {
                #                              id
                #                              name
                #                              ... on BusinessCapability {
                #                                relBusinessCapabilityToApplication {
                #                                  edges {
                #                                    node {
                #                                      id
                #                                      factSheet {
                #                                        id
                #                                        name
                #                                      }
                #                                    }
                #                                  }
                #                                }
                #                              }
                #                            }
                #                          }
                #                        }
                #                      }
                #                      """
                #
                # response_bc = requests.post(url=request_url, headers=headers,
                #                             json={"query": existing_relationships_query_bc})
                # response_bc.raise_for_status()
                # existing_relationships_bc = response_bc.json()['data']['allFactSheets']['edges']
                #
                # existing_relationships_bc_set = set()
                # for bc in existing_relationships_bc:
                #     bc_id = bc['node']['id']
                #     for relationship in bc['node']['relBusinessCapabilityToApplication']['edges']:
                #         app_id = relationship['node']['factSheet']['id']
                #         existing_relationships_bc_set.add((bc_id, app_id))
                #
                # # Prepare unique relationship data before making the API calls
                # relationship_data_bc = []
                #
                # for module in modules_data:
                #     app_name = f"{module['acronym']}({module['sid']}-{module['Client_id']})".strip()
                #     app_id = applications.get(app_name)
                #     if app_id:
                #         for bc_name, bc_id in business_capabilities.items():
                #             if bc_name == module['name']:
                #                 if (bc_id, app_id) not in existing_relationships_bc_set:
                #                     relationship_data_bc.append({
                #                         "Business Capability ID": bc_id,
                #                         "Business Capability Name": bc_name,
                #                         "Application ID": app_id,
                #                         "Application Name": app_name
                #                     })
                #
                # # Convert relationship data to a DataFrame
                # relationship_df_bc = pd.DataFrame(relationship_data_bc)
                #
                # # Drop duplicates to ensure unique relationships
                # relationship_df_bc = relationship_df_bc.drop_duplicates()
                #
                # # Proceed with the API calls to create the unique relationships
                # for _, relationship in relationship_df_bc.iterrows():
                #     bc_id = relationship["Business Capability ID"]
                #     bc_name = relationship["Business Capability Name"]
                #     app_id = relationship["Application ID"]
                #     app_name = relationship["Application Name"]
                #     create_relationship(app_id, app_name, bc_id, bc_name)
                #
                # # # ==================================================== Data Objects ======================

                # Load the Excel data
                file_path = 'data_object.xlsx'
                excel_data = pd.read_excel(file_path)

                # Extract the IDOC message types and their corresponding Langtext_EN
                idoc_data = excel_data[['IDOC_Message_Type', 'Langtext_EN']].to_dict(orient='records')

                # Fetch existing Data Objects
                def get_existing_data_objects():
                    query = """
                                                    {
                                                      allFactSheets(factSheetType: DataObject) {
                                                        edges {
                                                          node {
                                                            id
                                                            name
                                                          }
                                                        }
                                                      }
                                                    }
                                                    """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    return {data['node']['name']: data['node']['id'] for data in
                            response.json()['data']['allFactSheets']['edges']}

                existing_data_objects = get_existing_data_objects()

                # Function to create Data Object FactSheet if it does not exist
                def create_data_object(name, existing_data_objects):
                    if name not in existing_data_objects:
                        mutation6 = """
                                    mutation ($input: BaseFactSheetInput!, $patches: [Patch]!) {
                                      createFactSheet(input: $input, patches: $patches) {
                                        factSheet {
                                          name
                                          id
                                          tags {
                                            id
                                            name
                                          }
                                        }
                                      }
                                    }
                                                        """
                        variables6 = {
                            "input": {
                                "name": name,
                                "type": "DataObject"
                            },
                            "patches": [
                                {
                                    "op": "add",
                                    "path": "/tags",
                                    "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'
                                }
                            ],
                            "tagName": tag_name
                        }

                        data4 = {"query": mutation6, "variables": variables6}

                        response18 = requests.post(url=request_url, headers=headers, data=json.dumps(data4))
                        response18.raise_for_status()
                        result12 = response18.json()
                        new_id = result12['data']['createFactSheet']['factSheet']['id']
                        logs['DataObject'].append({
                            "Action": "Created",
                            "FactSheet Type": "DataObject",
                            "Name": name,
                            "ID": new_id
                        })
                        created_factsheets.append({
                            "Action": "Created",
                            "Name": name,
                            "FactSheet Type": "DataObject",
                            "ID": new_id
                        })
                        existing_data_objects[name] = new_id
                        print(f"Data Object FactSheet {name} created")

                # Process ALE data to create Data Object FactSheets
                def process_ale_data(ale_data, idoc_data, existing_data_objects):
                    idoc_dict = {item['IDOC_Message_Type']: item['Langtext_EN'] for item in idoc_data}
                    for ale_item in ale_data:
                        idoc_type = ale_item['idoc_messagetype']
                        if idoc_type in idoc_dict:
                            langtext_en = idoc_dict[idoc_type]

                            create_data_object(langtext_en, existing_data_objects)

                # Process the ALE data
                process_ale_data(Ale_data, idoc_data, existing_data_objects)

                # Extract the IDOC message types and their corresponding Langtext_EN
                idoc_data = excel_data[['IDOC_Message_Type', 'Langtext_EN']].to_dict(orient='records')

                # Create a dictionary from idoc_data
                idoc_dict = {item['IDOC_Message_Type']: item['Langtext_EN'] for item in idoc_data}

                # Fetch existing Data Object factsheets
                def get_existing_data_objects_with_tags():
                    query = """
                                                    {
                                                      allFactSheets(factSheetType: DataObject) {
                                                        edges {
                                                          node {
                                                            id
                                                            name
                                                            tags {
                                                              name
                                                            }
                                                          }
                                                        }
                                                      }
                                                    }
                                                    """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    return {data['node']['name']: {'id': data['node']['id'], 'tags': data['node']['tags']} for data
                            in
                            response.json()['data']['allFactSheets']['edges']}

                existing_data_objects_with_tags = get_existing_data_objects_with_tags()

                # Determine which Data Object factsheets are in the new data
                new_data_object_names = [idoc_dict[item['IDOC_Message_Type']] for item in idoc_data if
                                         item['IDOC_Message_Type'] in idoc_dict]

                # Identify Data Object factsheets to delete
                data_objects_to_delete = [
                    name for name in existing_data_objects_with_tags
                    if name not in new_data_object_names
                       and any(tag['name'] == tag_name for tag in existing_data_objects_with_tags[name]['tags'])
                ]

                # Function to delete Data Object factsheet
                def delete_data_object_factsheet(data_object_id, data_object_name):
                    mutation = """
                                                    mutation ($id: ID!) {
                                                        updateFactSheet(id: $id, comment: "Irrelevant Data Object", patches: [
                                                            {
                                                                op: add,
                                                                path: "/status",
                                                                value: "ARCHIVED"
                                                            }
                                                        ]) {
                                                            factSheet {
                                                                id
                                                                status
                                                            }
                                                        }
                                                    }
                                                    """
                    variables = {"id": data_object_id}
                    data = {"query": mutation, "variables": variables}
                    response = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                    response.raise_for_status()
                    print(f"Data Object factsheet {data_object_name} deleted")

                # Delete the identified Data Object factsheets
                for data_object_name in data_objects_to_delete:
                    data_object_id = existing_data_objects_with_tags[data_object_name]['id']
                    delete_data_object_factsheet(data_object_id, data_object_name)
                    logs['DataObject'].append({
                        "Action": "Deleted",
                        "FactSheet Type": "DataObject",
                        "Name": data_object_name,
                        "ID": data_object_id
                    })
                    deleted_factsheets.append({
                        "Name": data_object_name,
                        "FactSheet Type": "DataObject",
                        "ID": data_object_id
                    })

                # ---------------------------------------------- Interface ---------------------------------------

                # # Fetch all existing interfaces
                def fetch_all_interfaces():
                    query = """
                                           query {
                                             allFactSheets(factSheetType: Interface) {
                                               edges {
                                                 node {
                                                   id
                                                   name
                                                 }
                                               }
                                             }
                                           }
                                           """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    result = response.json()
                    return {edge['node']['name'].lower(): edge['node']['id'] for edge in
                            result['data']['allFactSheets']['edges']}

                existing_interfaces = fetch_all_interfaces()

                # Function to calculate similarity between two strings
                def similar(a, b):
                    if a is None or b is None:
                        return 0
                    return SequenceMatcher(None, a, b).ratio()

                # Match RFC data with Client data and Systems data
                def match_rfc_to_clients_and_systems(rfc_data, clients_data, systems_data):
                    matched_data4 = []
                    for rfc in rfc_data:
                        matched = False
                        for client in clients_data:
                            if similar(rfc['rfc_destination'], client['logical_system_name']) > 0.9:
                                sender = f"{rfc['sid']} {rfc['product_version']}"
                                receiver = f"{client['sid']}-{client['client_id']} {client['product_version']}"
                                if sender != receiver:
                                    interface_name = f"{sender} ->> {receiver}"
                                    matched_data4.append({
                                        'rfc_destination': rfc['rfc_destination'],
                                        'logical_system_name': client['logical_system_name'],
                                        'Interface_Name': interface_name,
                                        'Sender': sender,
                                        'Receiver': receiver

                                    })
                                matched = True
                                break
                        if not matched:
                            for system in systems_data:
                                if similar(rfc['target'], system['host_ip']) > 0.9:
                                    sender = f"{rfc['sid']} {rfc['product_version']}"
                                    receiver = f"{system['sid']} {system['product_version']}"
                                    if sender != receiver:
                                        interface_name = f"{sender} ->> {receiver}"
                                        matched_data4.append({
                                            'Interface_Name': interface_name,
                                            'Sender': sender,
                                            'Receiver': receiver,
                                            'target': rfc['target'],
                                            'host_ip': system['host_ip']
                                        })
                                    matched = True
                                    break
                        if not matched:
                            for system in systems_data:
                                if similar(rfc['target'], system['host_name']) > 0.9:
                                    sender = f"{rfc['sid']} {rfc['product_version']}"
                                    receiver = f"{system['sid']} {system['product_version']}"
                                    if sender != receiver:
                                        interface_name = f"{sender} ->> {receiver}"
                                        matched_data4.append({
                                            'Interface_Name': interface_name,
                                            'Sender': sender,
                                            'Receiver': receiver,
                                            'target': rfc['target'],
                                            'host_name': system['host_name']
                                        })
                                    break
                    return matched_data4

                # Assuming Rfc_data, clients_data, systems_data are defined elsewhere in your script
                matched_data = match_rfc_to_clients_and_systems(Rfc_data, clients_data, systems_data)

                # Convert matched data to DataFrame and drop duplicates
                matched_df = pd.DataFrame(matched_data)
                matched_df = matched_df.drop_duplicates()

                # Helper function to create interface fact sheets
                def create_interface_rfc_factsheet(interface, existing_interfaces):
                    interface_name_lower = interface['Interface_Name'].lower()
                    if interface_name_lower not in existing_interfaces:
                        mutation74 = """
                                                        mutation ($input: BaseFactSheetInput!, $patches: [Patch]!) {
                                                          createFactSheet(input: $input, patches: $patches) {
                                                            factSheet {
                                                              name
                                                              id
                                                              type
                                                              tags {
                                                                id
                                                                name
                                                              }
                                                            }
                                                          }
                                                        }
                                                        """
                        variables74 = {
                            "input": {
                                "name": interface['Interface_Name'],
                                "type": "Interface"
                            },
                            "patches": [
                                {
                                    "op": "add",
                                    "path": "/tags",
                                    "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'
                                }
                            ],
                            "tagName": tag_name
                        }

                        data74 = {"query": mutation74, "variables": variables74}
                        response = requests.post(url=request_url, headers=headers, json=data74)
                        response.raise_for_status()
                        result = response.json()
                        if 'errors' in result:
                            print(f"Error creating Interface FactSheet: {result['errors']}")
                        else:
                            new_interface = result['data']['createFactSheet']['factSheet']
                            print(f"RFC  Interface FactSheet: {new_interface['name']} created")
                            existing_interfaces[interface_name_lower] = new_interface['id']
                            return new_interface['id'], new_interface['name']
                    else:
                        return existing_interfaces[interface_name_lower], interface['Interface_Name']
                    return None, None

                # List to hold new interface IDs and names
                new_interfaces = []

                for _, interface in matched_df.iterrows():
                    existing_interfaces = fetch_all_interfaces()  # Refresh the existing interfaces before each creation attempt
                    interface_id, interface_name = create_interface_rfc_factsheet(interface, existing_interfaces)
                    if interface_id and interface_name:
                        new_interfaces.append({
                            'Interface ID': interface_id,
                            'Interface Name': interface_name
                        })
                        # Log the creation action
                        logs['Interface'].append({
                            'Name': interface_name,
                            'Action': 'Created',
                            'FactSheet Type': 'Interface',
                            'ID': interface_id
                        })
                        created_factsheets.append({
                            'Name': interface_name,
                            'Action': 'Created',
                            'FactSheet Type': 'Interface',
                            'ID': interface_id
                        })

                # print("RFC Done")

                new_interfaces_df = pd.DataFrame(new_interfaces)
                rfc_interface = new_interfaces_df.drop_duplicates()

                # ALE Function to create an interface fact sheet
                def create_interface_factsheet(interface_name, existing_interfaces):
                    interface_name_lower = interface_name.lower()
                    if interface_name_lower not in existing_interfaces:
                        mutation55 = """
                                        mutation ($input: BaseFactSheetInput!, $patches: [Patch]!) {
                                          createFactSheet(input: $input, patches: $patches) {
                                            factSheet {
                                              name
                                              id
                                              type
                                              tags {
                                                id
                                                name
                                              }
                                            }
                                          }
                                        }
                                                        """
                        variables55 = {
                            "input": {
                                "name": interface_name,
                                "type": "Interface"
                            },
                            "patches": [
                                {
                                    "op": "add",
                                    "path": "/tags",
                                    "value": f'[{{"tagName":"{tag_name}"}}, {{"tagName":"{tag_name_2}"}}]'
                                }
                            ],
                            "tagName": tag_name
                        }

                        data55 = {"query": mutation55, "variables": variables55}
                        response = requests.post(url=request_url, headers=headers, json=data55)
                        response.raise_for_status()
                        result = response.json()
                        if 'errors' in result:
                            print(f"Error creating Interface FactSheet: {result['errors']}")
                        else:
                            new_interface = result['data']['createFactSheet']['factSheet']
                            print(f" ALE Interface factSheet  {new_interface['name']} created")
                            existing_interfaces[interface_name_lower] = new_interface['id']
                            return new_interface['id'], new_interface['name']
                    else:
                        return existing_interfaces[interface_name_lower], interface_name
                    return None, None

                # Helper function to create interface details from ALE data
                def create_interfaces(clients_data2, ale_data2):
                    interfaces5 = []
                    for ale in ale_data2:
                        sender_matches = [client for client in clients_data2 if
                                          client['logical_system_name'] == ale['sender']]
                        receiver_matches = [client for client in clients_data2 if
                                            client['logical_system_name'] == ale['receiver']]
                        for sender in sender_matches:
                            for receiver in receiver_matches:
                                interface_name = f"{sender['sid']}-{sender['client_id']} {sender['product_version']} ->> {receiver['sid']}-{receiver['client_id']} {receiver['product_version']}"
                                interfaces5.append({
                                    "interface_name": interface_name,
                                    "idoc_messagetype": ale['idoc_messagetype']
                                })
                    return interfaces5

                # Assuming clients_data and Ale_data are defined elsewhere in your script
                interfaces = create_interfaces(clients_data, Ale_data)

                # Convert interfaces to DataFrame and drop duplicates
                matched_df2 = pd.DataFrame(interfaces)
                matched_df2 = matched_df2.drop_duplicates()

                # List to hold new interface IDs and names
                new_ale_interfaces = []

                for _, interface in matched_df2.iterrows():
                    existing_interfaces = fetch_all_interfaces()  # Refresh the existing interfaces before each creation attempt
                    interface_id, interface_name = create_interface_factsheet(interface['interface_name'],
                                                                              existing_interfaces)
                    logs['Interface'].append({
                        "Action": "Created",
                        "FactSheet Type": "Interface",
                        "Name": interface_name,
                        "ID": interface_id
                    })
                    created_factsheets.append({
                        "Action": "Created",
                        "Name": interface_name,
                        "FactSheet Type": "Interface",
                        "ID": interface_id
                    })

                    if interface_id and interface_name:
                        new_ale_interfaces.append({
                            'Interface ID': interface_id,
                            'Interface Name': interface_name
                        })

                # Convert the list of new ALE interfaces to a DataFrame
                ale_interface = pd.DataFrame(new_ale_interfaces)
                ale_interface = ale_interface.drop_duplicates()

                # Fetch all existing interfaces
                def fetch_all_interfaces():
                    query = """
                                                    query {
                                                      allFactSheets(factSheetType: Interface) {
                                                        edges {
                                                          node {
                                                            id
                                                            name
                                                            tags {
                                                              name
                                                            }
                                                          }
                                                        }
                                                      }
                                                    }
                                                    """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    result = response.json()
                    return {edge['node']['name']: {'id': edge['node']['id'], 'tags': edge['node']['tags']} for edge
                            in
                            result['data']['allFactSheets']['edges']}

                # Helper function to delete interfaces
                def delete_interface(interface_id, interface_name):
                    mutation = """
                                                    mutation ($id: ID!) {
                                                        updateFactSheet(id: $id, comment: "Irrelevant interface", patches: [
                                                            {
                                                                op: add,
                                                                path: "/status",
                                                                value: "ARCHIVED"
                                                            }
                                                        ]) {
                                                            factSheet {
                                                                id
                                                                status
                                                            }
                                                        }
                                                    }
                                                    """
                    variables = {"id": interface_id}
                    data = {"query": mutation, "variables": variables}
                    response = requests.post(url=request_url, headers=headers, data=json.dumps(data))
                    response.raise_for_status()
                    print(f"Interface factsheet {interface_name} deleted")

                # Fetch all existing interfaces
                existing_interfaces = fetch_all_interfaces()

                # Create a set of new interface names from matched RFC and ALE data
                new_interface_names_rfc = set(matched_df['Interface_Name'].str.lower())
                new_interface_names_ale = set(matched_df2['interface_name'].str.lower())

                # Combine the new interface names from both RFC and ALE
                new_interface_names = new_interface_names_rfc.union(new_interface_names_ale)

                # Identify interfaces to delete: those tagged with "LA System" and not in the new data
                interfaces_to_delete = [name for name, details in existing_interfaces.items()
                                        if tag_name in [tag['name'] for tag in
                                                        details[
                                                            'tags']] and name.lower() not in new_interface_names]

                # Perform the deletion
                for interface_name in interfaces_to_delete:
                    interface_id = existing_interfaces[interface_name]['id']
                    delete_interface(interface_id, interface_name)
                    logs['Interface'].append({
                        "Action": "Deleted",
                        "FactSheet Type": "Interface",
                        "Name": interface_name,
                        "ID": interface_id
                    })
                    deleted_factsheets.append({
                        "Action": "Deleted",
                        "FactSheet Type": "Interface",
                        "Name": interface_name,
                        "ID": interface_id
                    })

                # Fetch all fact sheets of a given type
                def fetch_all_fact_sheets(fact_sheet_type):
                    query = f"""
                                                       {{
                                                         allFactSheets(factSheetType: {fact_sheet_type}) {{
                                                           edges {{
                                                             node {{
                                                               id
                                                               name
                                                             }}
                                                           }}
                                                         }}
                                                       }}
                                                       """
                    response = requests.post(url=request_url, headers=headers, json={"query": query})
                    response.raise_for_status()
                    return {node['node']['name']: node['node']['id'] for node in
                            response.json()['data']['allFactSheets']['edges']}

                # Fetch all Data Objects and Interfaces again to ensure we have the latest IDs
                data_objects = fetch_all_fact_sheets("DataObject")
                interfaces = fetch_all_fact_sheets("Interface")

                # # Create relationships between Data Objects and Interfaces
                # def create_relationship_interface(data_object_id, data_object_name, interface_id, interface_name):
                #     if data_object_id and interface_id:
                #         mutation = f'''
                #                                            mutation {{
                #                                              updateFactSheet(id: "{data_object_id}", patches: [
                #                                                {{
                #                                                  op: add,
                #                                                  path: "/relDataObjectToInterface/new_1",
                #                                                  value: "{{\\"factSheetId\\":\\"{interface_id}\\"}}"
                #                                                }}
                #                                              ]) {{
                #                                                factSheet {{
                #                                                  id
                #                                                  ... on DataObject {{
                #                                                    relDataObjectToInterface {{
                #                                                      edges {{
                #                                                        node {{
                #                                                          id
                #                                                        }}
                #                                                      }}
                #                                                    }}
                #                                                  }}
                #                                                }}
                #                                              }}
                #                                            }}
                #                                            '''
                #         response = requests.post(url=request_url, headers=headers, json={"query": mutation})
                #         if response.ok:
                #             print(
                #                 f"Relationship created between Data Object factsheet '{data_object_name}' and Interface factsheet '{interface_name}'")
                #         else:
                #             print(f"Failed to create relationship: {response.text}")
                #
                # # Fetch existing relationships (Data Object to Interface)
                # def fetch_existing_relationships_data_object_to_interface():
                #     query = """
                #                                        {
                #                                          allFactSheets(factSheetType: DataObject) {
                #                                            edges {
                #                                              node {
                #                                                id
                #                                                name
                #                                                ... on DataObject {
                #                                                  relDataObjectToInterface {
                #                                                    edges {
                #                                                      node {
                #                                                        id
                #                                                        factSheet {
                #                                                          id
                #                                                          name
                #                                                        }
                #                                                      }
                #                                                    }
                #                                                  }
                #                                                }
                #                                              }
                #                                            }
                #                                          }
                #                                        }
                #                                        """
                #     response = requests.post(url=request_url, headers=headers, json={"query": query})
                #     response.raise_for_status()
                #     result = response.json()
                #     relationships = set()
                #     for edge in result['data']['allFactSheets']['edges']:
                #         data_object_id = edge['node']['id']
                #         for rel in edge['node']['relDataObjectToInterface']['edges']:
                #             interface_id = rel['node']['factSheet']['id']
                #             relationships.add((data_object_id, interface_id))
                #     return relationships
                #
                # existing_relationships_data_object_to_interface = fetch_existing_relationships_data_object_to_interface()
                #
                # # Prepare relationship data between Data Objects and Interfaces
                # relationship_data_object_to_interface = []
                # idoc_dict = {item['IDOC_Message_Type']: item['Langtext_EN'] for item in idoc_data}
                #
                # for ale in Ale_data:
                #     data_object_name = idoc_dict.get(ale['idoc_messagetype'])
                #     if data_object_name:
                #         data_object_id = data_objects.get(data_object_name)
                #         sender_matches = [client for client in clients_data if
                #                           client['logical_system_name'] == ale['sender']]
                #         receiver_matches = [client for client in clients_data if
                #                             client['logical_system_name'] == ale['receiver']]
                #         for sender in sender_matches:
                #             for receiver in receiver_matches:
                #                 interface_name = f"{sender['sid']}-{sender['client_id']} {sender['product_version']} ->> {receiver['sid']}-{receiver['client_id']} {receiver['product_version']}"
                #                 interface_id = interfaces.get(interface_name)
                #                 if data_object_id and interface_id and (
                #                         data_object_id,
                #                         interface_id) not in existing_relationships_data_object_to_interface:
                #                     relationship_data_object_to_interface.append({
                #                         "Data Object ID": data_object_id,
                #                         "Interface ID": interface_id,
                #                         "Data Object Name": data_object_name,
                #                         "Interface Name": interface_name
                #                     })
                #
                # # Convert relationship data to a DataFrame
                # relationship_df_data_object_to_interface = pd.DataFrame(relationship_data_object_to_interface)
                # relationship_df_data_object_to_interface = relationship_df_data_object_to_interface.drop_duplicates()
                #
                # # Proceed with the API calls to create the unique relationships
                # for _, relationship in relationship_df_data_object_to_interface.iterrows():
                #     create_relationship_interface(relationship["Data Object ID"],
                #                                   relationship["Data Object Name"],
                #                                   relationship["Interface ID"],
                #                                   relationship["Interface Name"])
                #
                # # Function to fetch all applications
                # def fetch_all_applications():
                #     query = """
                #                                    {
                #                                      allFactSheets(factSheetType: Application) {
                #                                        edges {
                #                                          node {
                #                                            id
                #                                            name
                #                                          }
                #                                        }
                #                                      }
                #                                    }
                #                                    """
                #     response = requests.post(url=request_url, headers=headers, json={"query": query})
                #     response.raise_for_status()
                #     return {node['node']['name']: node['node']['id'] for node in
                #             response.json()['data']['allFactSheets']['edges']}
                #
                # applications = fetch_all_applications()
                #
                # # Function to create relationships between Interfaces and Applications
                # def create_application_interface_relationship(interface_id, provider_id, consumer_id,
                #                                               interface_name,
                #                                               provider_name,
                #                                               consumer_name):
                #     mutation11 = f'''
                #                                        mutation {{
                #                                          updateFactSheet(id: "{interface_id}", patches: [
                #                                            {{
                #                                              op: add,
                #                                              path: "/relInterfaceToConsumerApplication/new_1",
                #                                              value: "{{\\"factSheetId\\":\\"{consumer_id}\\"}}"
                #                                            }}, {{
                #                                              op: add,
                #                                              path: "/relInterfaceToProviderApplication/new",
                #                                              value: "{{\\"factSheetId\\":\\"{provider_id}\\"}}"
                #                                            }}
                #                                          ]) {{
                #                                            factSheet {{
                #                                              id
                #                                              ... on Interface {{
                #                                                relInterfaceToConsumerApplication {{
                #                                                  edges {{
                #                                                    node {{
                #                                                      id
                #                                                    }}
                #                                                  }}
                #                                                }},
                #                                                relInterfaceToProviderApplication {{
                #                                                  edges {{
                #                                                    node {{
                #                                                      id
                #                                                    }}
                #                                                  }}
                #                                                }}
                #                                              }}
                #                                            }}
                #                                          }}
                #                                        }}
                #                                        '''
                #     response = requests.post(url=request_url, headers=headers, json={"query": mutation11})
                #     response.raise_for_status()
                #     if response.ok:
                #         print(
                #             f"Relationships created for Interface factsheet '{interface_name}' with Provider interface factsheet'{provider_name}' and Consumer interface factsheet'{consumer_name}'")
                #     else:
                #         print(f"Failed to create relationships: {response.text}")
                #
                # # Function to fetch existing relationships (Interface to Application)
                # def fetch_existing_relationships_interface_to_application():
                #     query = """
                #                                    {
                #                                      allFactSheets(factSheetType: Interface) {
                #                                        edges {
                #                                          node {
                #                                            id
                #                                            name
                #                                            ... on Interface {
                #                                              relInterfaceToConsumerApplication {
                #                                                edges {
                #                                                  node {
                #                                                    id
                #                                                    factSheet {
                #                                                      id
                #                                                      name
                #                                                    }
                #                                                  }
                #                                                }
                #                                              }
                #                                              relInterfaceToProviderApplication {
                #                                                edges {
                #                                                  node {
                #                                                    id
                #                                                    factSheet {
                #                                                      id
                #                                                      name
                #                                                    }
                #                                                  }
                #                                                }
                #                                              }
                #                                            }
                #                                          }
                #                                        }
                #                                      }
                #                                    }
                #                                    """
                #     response = requests.post(url=request_url, headers=headers, json={"query": query})
                #     response.raise_for_status()
                #     result = response.json()
                #     relationships = set()
                #     for edge in result['data']['allFactSheets']['edges']:
                #         interface_id = edge['node']['id']
                #         for rel in edge['node']['relInterfaceToConsumerApplication']['edges']:
                #             consumer_id = rel['node']['factSheet']['id']
                #             relationships.add((interface_id, consumer_id))
                #         for rel in edge['node']['relInterfaceToProviderApplication']['edges']:
                #             provider_id = rel['node']['factSheet']['id']
                #             relationships.add((interface_id, provider_id))
                #     return relationships
                #
                # existing_relationships_interface_to_application = fetch_existing_relationships_interface_to_application()
                #
                # # Prepare relationship data for Interfaces and Applications
                # relationship_interface_to_application = []
                # for interface_name, interface_id in interfaces.items():
                #     split_name = interface_name.split(" ->> ")
                #     if len(split_name) == 2:
                #         provider_name = split_name[0].strip()
                #         consumer_name = split_name[1].strip()
                #         provider_id = applications.get(provider_name)
                #         consumer_id = applications.get(consumer_name)
                #         if provider_id and consumer_id and (
                #                 interface_id,
                #                 provider_id) not in existing_relationships_interface_to_application and (
                #                 interface_id, consumer_id) not in existing_relationships_interface_to_application:
                #             relationship_interface_to_application.append({
                #                 "Interface ID": interface_id,
                #                 "Provider ID": provider_id,
                #                 "Consumer ID": consumer_id,
                #                 "Interface Name": interface_name,
                #                 "Provider Name": provider_name,
                #                 "Consumer Name": consumer_name
                #             })
                #
                # # Convert relationship data to a DataFrame
                # relationship_df_interface_to_application = pd.DataFrame(relationship_interface_to_application)
                #
                # # Drop duplicates to ensure unique relationships
                # relationship_df_interface_to_application = relationship_df_interface_to_application.drop_duplicates()
                #
                # # Proceed with the API calls to create the unique relationships
                # for _, relationship in relationship_df_interface_to_application.iterrows():
                #     create_application_interface_relationship(
                #         relationship["Interface ID"], relationship["Provider ID"], relationship["Consumer ID"],
                #         relationship["Interface Name"], relationship["Provider Name"], relationship["Consumer Name"]
                #     )
                #
                # # Function to fetch existing relationships (Data Object to Application)
                # def fetch_existing_relationships_data_object_to_application():
                #     query = """
                #                                        {
                #                                          allFactSheets(factSheetType: DataObject) {
                #                                            edges {
                #                                              node {
                #                                                id
                #                                                name
                #                                                ... on DataObject {
                #                                                  relDataObjectToApplication {
                #                                                    edges {
                #                                                      node {
                #                                                        id
                #                                                        factSheet {
                #                                                          id
                #                                                          name
                #                                                        }
                #                                                      }
                #                                                    }
                #                                                  }
                #                                                }
                #                                              }
                #                                            }
                #                                          }
                #                                        }
                #                                        """
                #     response = requests.post(url=request_url, headers=headers, json={"query": query})
                #     response.raise_for_status()
                #     result = response.json()
                #     relationships = set()
                #     for edge in result['data']['allFactSheets']['edges']:
                #         data_object_id1 = edge['node']['id']
                #         for rel in edge['node']['relDataObjectToApplication']['edges']:
                #             application_id1 = rel['node']['factSheet']['id']
                #             relationships.add((data_object_id1, application_id1))
                #     return relationships
                #
                # existing_relationships_data_object_to_application = fetch_existing_relationships_data_object_to_application()
                #
                # # Prepare relationship data between Data Objects and Applications
                # relationship_data_object_to_application = []
                #
                # # Assuming 'idoc_dict' is defined and maps IDOC Message Types to Data Object names
                # idoc_dict = {item['IDOC_Message_Type']: item['Langtext_EN'] for item in idoc_data}
                #
                # for ale in Ale_data:
                #     data_object_name = idoc_dict.get(ale['idoc_messagetype'])
                #     if data_object_name:
                #         data_object_id = data_objects.get(data_object_name)
                #         sender_matches = [client for client in clients_data if
                #                           client['logical_system_name'] == ale['sender']]
                #         receiver_matches = [client for client in clients_data if
                #                             client['logical_system_name'] == ale['receiver']]
                #
                #         # Create relationships for senders
                #         for sender in sender_matches:
                #             application_name = f"{sender['sid']}-{sender['client_id']} {sender['product_version']}"
                #             application_id = applications.get(application_name)
                #             if data_object_id and application_id and (
                #                     data_object_id,
                #                     application_id) not in existing_relationships_data_object_to_application:
                #                 relationship_data_object_to_application.append({
                #                     "Data Object ID": data_object_id,
                #                     "Application ID": application_id,
                #                     "Data Object Name": data_object_name,
                #                     "Application Name": application_name
                #                 })
                #
                #         # Create relationships for receivers
                #         for receiver in receiver_matches:
                #             application_name = f"{receiver['sid']}-{receiver['client_id']} {receiver['product_version']}"
                #             application_id = applications.get(application_name)
                #             if data_object_id and application_id and (
                #                     data_object_id,
                #                     application_id) not in existing_relationships_data_object_to_application:
                #                 relationship_data_object_to_application.append({
                #                     "Data Object ID": data_object_id,
                #                     "Application ID": application_id,
                #                     "Data Object Name": data_object_name,
                #                     "Application Name": application_name
                #                 })
                #
                # # Convert relationship data to a DataFrame
                # relationship_df_data_object_to_application = pd.DataFrame(relationship_data_object_to_application)
                # relationship_df_data_object_to_application = relationship_df_data_object_to_application.drop_duplicates()
                #
                # # Helper function to create relationships between Data Object and Application
                # def create_data_object_application_relationship(data_object_id, application_id
                #                                                 ):
                #     mutation55 = f'''
                #                                        mutation {{
                #                                          updateFactSheet(id: "{data_object_id}", patches: [
                #                                            {{
                #                                              op: add,
                #                                              path: "/relDataObjectToApplication/new_1",
                #                                              value: "{{\\"factSheetId\\":\\"{application_id}\\"}}"
                #                                            }}
                #                                          ]) {{
                #                                            factSheet {{
                #                                              id
                #                                              ... on DataObject {{
                #                                                relDataObjectToApplication {{
                #                                                  edges {{
                #                                                    node {{
                #                                                      id
                #                                                    }}
                #                                                  }}
                #                                                }}
                #                                              }}
                #                                            }}
                #                                          }}
                #                                        }}
                #                                        '''
                #     response = requests.post(url=request_url, headers=headers, json={"query": mutation55})
                #     response.raise_for_status()
                #
                # # Proceed with the API calls to create the unique relationships
                # for _, relationship in relationship_df_data_object_to_application.iterrows():
                #     create_data_object_application_relationship(relationship["Data Object ID"],
                #                                                 relationship["Application ID"])
                #     print(
                #         f"Created relationship between Data Object factsheet {relationship["Data Object Name"]} and Application factsheet {relationship["Application Name"]}")
                #
                # # # Match RFC data with Clients and ALE data
                # def match_rfc_to_ale(rfc_data, clients_data, ale_data):
                #     matched_data = []
                #     for rfc in rfc_data:
                #         for client in clients_data:
                #             if similar(rfc['rfc_destination'], client['logical_system_name']) > 0.9:
                #                 for ale in ale_data:
                #                     if ale['sender'] == client['logical_system_name'] or ale['receiver'] == client[
                #                         'logical_system_name']:
                #                         matched_data.append({
                #                             'IDOC_Message_Type': ale['idoc_messagetype'],
                #                             'Data Object Name': idoc_dict.get(ale['idoc_messagetype']),
                #                             'Application Name': f"{ale['sid']} {ale['product_version']}"
                #                         })
                #     return matched_data
                #
                # # Assuming Rfc_data, clients_data, and Ale_data are already defined
                # matched_data5 = match_rfc_to_ale(Rfc_data, clients_data, Ale_data)
                #
                # # Convert matched data to DataFrame and drop duplicates
                # matched_df5 = pd.DataFrame(matched_data5)
                # matched_df5 = matched_df5.drop_duplicates()
                #
                # # Remove rows where "Data Object Name" is not available
                # matched_df5 = matched_df5.dropna(subset=['Data Object Name'])
                #
                # # # Save relationship data to an Excel file
                # # output_file_path = 'data_object_to_application_relationships_additional.xlsx'
                # # matched_df5.to_excel(output_file_path, index=False)
                # #
                # # print(f"Relationship data saved to {output_file_path}")
                #
                # # Fetch all Data Objects and Applications again to ensure we have the latest IDs
                # data_objects = fetch_all_fact_sheets("DataObject")
                # applications = fetch_all_applications()
                #
                # # Proceed with the API calls to create the unique relationships
                # for _, relationship in matched_df5.iterrows():
                #     data_object_name = relationship["Data Object Name"]
                #     application_name = relationship["Application Name"]
                #     data_object_id = data_objects.get(data_object_name)
                #     application_id = applications.get(application_name)
                #     if data_object_id and application_id and (
                #             data_object_id,
                #             application_id) not in existing_relationships_data_object_to_application:
                #         create_data_object_application_relationship(data_object_id, application_id)
                #         print(
                #             f"Created relationships between Data Objects factsheet {data_object_name} and Applications factsheet {application_name}.")
                #
                # # New logic to create relationships between Interface and Data Object fact sheets
                # def match_rfc_to_ale_for_additional_relationships(rfc_data, clients_data, ale_data):
                #     matched_data = []
                #     for rfc in rfc_data:
                #         for client in clients_data:
                #             if similar(rfc['rfc_destination'], client['logical_system_name']) > 0.9:
                #                 for ale in ale_data:
                #                     if ale['sender'] == client['logical_system_name'] or ale['receiver'] == client[
                #                         'logical_system_name']:
                #                         interface_name = f"{ale['sid']} {ale['product_version']} ->> {client['sid']}-{client['client_id']} {client['product_version']}"
                #                         matched_data.append({
                #                             'IDOC_Message_Type': ale['idoc_messagetype'],
                #                             'Data Object Name': idoc_dict.get(ale['idoc_messagetype']),
                #                             'Interface Name': interface_name
                #                         })
                #     return matched_data
                #
                # # Assuming Rfc_data, clients_data, and Ale_data are already defined
                # additional_relationships_data = match_rfc_to_ale_for_additional_relationships(Rfc_data,
                #                                                                               clients_data,
                #                                                                               Ale_data)
                #
                # # Convert matched data to DataFrame and drop duplicates
                # additional_relationships_df = pd.DataFrame(additional_relationships_data)
                # additional_relationships_df = additional_relationships_df.drop_duplicates()
                #
                # # Remove rows where "Data Object Name" or "Interface Name" is not available
                # additional_relationships_df = additional_relationships_df.dropna(
                #     subset=['Data Object Name', 'Interface Name'])
                #
                # # # Create additional relationships between Interface and Data Object fact sheets
                # def create_data_object_interface_relationship(data_object_id, interface_id):
                #     mutation = f'''
                #                         mutation {{
                #                           updateFactSheet(id: "{data_object_id}", patches: [
                #                             {{
                #                               op: add,
                #                               path: "/relDataObjectToInterface/new_1",
                #                               value: "{{\\"factSheetId\\":\\"{interface_id}\\"}}"
                #                             }}
                #                           ]) {{
                #                             factSheet {{
                #                               id
                #                               ... on DataObject {{
                #                                 relDataObjectToInterface {{
                #                                   edges {{
                #                                     node {{
                #                                       id
                #                                     }}
                #                                   }}
                #                                 }}
                #                               }}
                #                             }}
                #                           }}
                #                         }}
                #                         '''
                #     response = requests.post(url=request_url, headers=headers, json={"query": mutation})
                #     response.raise_for_status()
                #
                # # Fetch all Data Objects and Interfaces again to ensure we have the latest IDs
                # data_objects = fetch_all_fact_sheets("DataObject")
                # interfaces = fetch_all_fact_sheets("Interface")
                #
                # for i in range(2):
                #     # Proceed with the API calls to create the unique relationships
                #     for _, relationship in relationship_df_clients.iterrows():
                #         parent_id = relationship["Parent Application ID"]
                #         child_id = relationship["Child Application ID"]
                #
                #         relationship_mutation = f'''
                #                                     mutation {{
                #                                       updateFactSheet(id: "{parent_id}", patches: [
                #                                         {{
                #                                           op: add,
                #                                           path: "/relToChild/new_1",
                #                                           value: "{{\\"factSheetId\\":\\"{child_id}\\"}}"
                #                                         }}
                #                                       ]) {{
                #                                         factSheet {{
                #                                           id
                #                                           name
                #                                           ... on Application {{
                #                                             relToChild {{
                #                                               edges {{
                #                                                 node {{
                #                                                   id
                #                                                 }}
                #                                               }}
                #                                             }}
                #                                           }}
                #                                         }}
                #                                       }}
                #                                     }}
                #                                     '''
                #         response15 = requests.post(url=request_url, headers=headers,
                #                                    data=json.dumps({"query": relationship_mutation}))
                #         response15.raise_for_status()
                #
                #         print(
                #             f"Created relationship between Parent Application '{relationship['Parent Application Name']}' and Child Application '{relationship['Child Application Name']}'")
                #
                # # Proceed with the API calls to create the additional unique relationships
                # for _, relationship in additional_relationships_df.iterrows():
                #     data_object_name = relationship["Data Object Name"]
                #     interface_name = relationship["Interface Name"]
                #     data_object_id = data_objects.get(data_object_name)
                #     interface_id = interfaces.get(interface_name)
                #     if data_object_id and interface_id and (
                #             data_object_id, interface_id) not in existing_relationships_data_object_to_interface:
                #         create_data_object_interface_relationship(data_object_id, interface_id)
                #         print(
                #             f"Created relationship between Data Object factsheet '{data_object_name}' and Interface '{interface_name}'")

        except Exception as e:
            print(f"An error occurred: {e}")
            save_factsheet_logs_to_excel(logs)
            raise

        save_created_updated_factsheets_to_excel(created_factsheets, updated_factsheets, deleted_factsheets)

        operation_logs_path = save_factsheet_logs_to_excel2(logs, "Operation", test_mode=False)
        return operation_logs_path

