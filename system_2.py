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

        systems_data = parse_systems_xml(System_xml_content)
        components_data = parse_components_xml(components_xml_content)
        hosts_data = parse_Hosts_xml(Hosts_xml_content)
        clients_data = parse_clients_xml(Clients_xml_content)
        modules_data = parse_modules_xml(modules_xml_content)
        Ale_data = parse_Ale_xml(Ale_xml_content)
        Rfc_data = parse_Rfc_xml(Rfc_xml_content)

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

        # Example: Deleting factsheets and logging the deletions

        if create_factsheet:
            os.makedirs(operation_folder, exist_ok=True)
            excel_file_path = os.path.join(operation_folder, 'created_factsheets.xlsx')

            user_canceled = False  # Flag to check if the user canceled the operation

            try:
                for _ in range(3):  # Loop to run the process three times
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
                        # Check if user canceled
                        if user_canceled:
                            print("User canceled operation. Saving logs and exiting...")
                            save_factsheet_logs_to_excel(logs, "created_factsheets_cancelled")
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

                    # Repeat the same logic for creating other types of fact sheets (e.g., ITComponent, TechnicalStack)

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
                        # Check if user canceled
                        if user_canceled:
                            print("User canceled operation. Saving logs and exiting...")
                            save_factsheet_logs_to_excel(logs, "created_factsheets_cancelled")
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

                        # Check if user canceled
                        if user_canceled:
                            print("User canceled operation. Saving logs and exiting...")
                            save_factsheet_logs_to_excel(logs, "created_factsheets_cancelled")
                            return excel_file_path



                # Retrieve all applications and IT components again after creation
                applications_response = requests.post(url=request_url, headers=headers,
                                                      json={"query": application_query})
                applications_response.raise_for_status()
                applications = {app['node']['displayName']: app['node']['id'] for app in
                                applications_response.json()['data']['allFactSheets']['edges']}

                components_response = requests.post(url=request_url, headers=headers, json={"query": component_query})
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
                        f"Created relationship from Application '{relationship['Application Name']}' to IT Component '{relationship['IT Component Name']}'")

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

                    response6 = requests.post(url=request_url, headers=headers, json={"query": technical_stack_query})
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

                for host in hosts_data:
                    host_name = host['name']
                    ip_address = host['ip_address']

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
                        response10.raise_for_status()
                        print(f"IT Component factsheet {host_name} Created")
                        it2 = response10.json()
                        it_2 = it2['data']['createFactSheet']['factSheet']['id']

                        # Log the creation action
                        logs['ITComponent'].append({
                            'Name': host_name,
                            'Action': 'Created',
                            'FactSheet Type': 'ITComponent',
                            'ID': it_2
                        })

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
                        it_component_name = client['sid'] + "-" + client['client_id'] + ' ' + client['product_version']
                        description = client['description']

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

                    response18 = requests.post(url=request_url, headers=headers, data=json.dumps(data3))
                    response18.raise_for_status()
                    print(f"Business Capability FactSheet {name} Created")
                    bcid = response18.json()
                    bc__id = bcid['data']['createFactSheet']['factSheet']['id']

                    # Log the creation action
                    logs['BusinessCapability'].append({
                        'Name': name,
                        'Action': 'Created',
                        'FactSheet Type': 'BusinessCapability',
                        'ID': bc__id  # Include the ID in the log
                    })

                # Process the modules data and create Business Capabilities
                def process_modules_data(modules_data1, existing_business_capabilities1):
                    for module in modules_data1:
                        module_name = module['name']
                        if module_name not in existing_business_capabilities1:
                            create_business_capability(module_name)

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
                    print(f"Created relationship from Application '{app_name}' to Business Capability '{biz_cap_name}'")

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
                        if idoc_type in idoc_dict:
                            langtext_en = idoc_dict[idoc_type]

                            create_data_object(langtext_en, existing_data_objects)

                # Process the ALE data
                process_ale_data(Ale_data, idoc_data, existing_data_objects)

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
                            print(f"Interface factsheet {interface['Interface_Name']} created")

                # Iterate through the matched interfaces and create fact sheets if they do not exist
                for _, interface in matched_df.iterrows():
                    existing_interfaces = fetch_all_interfaces()  # Refresh the existing interfaces before each creation attempt
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
                            print(f"Interface FactSheet {new_interface['name']} Created")
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
                def create_relationship_interface(data_object_id, data_object_name, interface_id, interface_name):
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
                    create_relationship_interface(relationship["Data Object ID"], relationship["Data Object Name"],
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
                def create_application_interface_relationship(interface_id, provider_id, consumer_id, interface_name,
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
                                interface_id, provider_id) not in existing_relationships_interface_to_application and (
                                interface_id, consumer_id) not in existing_relationships_interface_to_application:
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
                save_factsheet_logs_to_excel(logs, "created_factsheets_error")
                raise

            # After creating all applications, save the logs to an Excel file
            file_path5 = save_factsheet_logs_to_excel(logs, "created_factsheets", test_mode=False)
            return file_path5

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
                        # save_factsheet_logs_to_excel(logs_before_deletion)

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
        save_logs_and_continue(e)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        save_logs_and_continue(e)


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
