import platform
import subprocess
import sys
import os
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QFrame, QPushButton,
                             QGraphicsView, QScrollArea, QDialog, QLineEdit, QFileDialog, QMessageBox, QProgressBar,
                             QTextEdit, QComboBox, QListWidget, QListWidgetItem, QSplashScreen, QCheckBox, QRadioButton,
                             QButtonGroup)
from PyQt5.QtGui import QIcon, QPixmap, QMouseEvent, QTransform
from PyQt5.QtCore import Qt, pyqtSignal, QObject, QThread, QTimer, QElapsedTimer, QUrl
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import requests
import time
from system import process_system_analysis
from PyQt5.QtWidgets import QTableWidget, QTableWidgetItem
from PyQt5.QtGui import QBrush, QColor
import shutil
from PyQt5.QtWidgets import QInputDialog, QMessageBox, QFileDialog
from PyQt5.QtGui import QBrush, QColor
from PyQt5.QtWidgets import QSystemTrayIcon


class SignalEmitter(QObject):
    text_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    process_finished = pyqtSignal()
    auth_failed = pyqtSignal()
    no_files_found = pyqtSignal()
    file_check_failed = pyqtSignal(str)
    unexpected_error = pyqtSignal(str)
    connection_error = pyqtSignal()  # Add this signal


# Load the Excel data
file_path = 'data_object.xlsx'
excel_data = pd.read_excel(file_path)

# Extract the IDOC message types and their corresponding Langtext_EN
idoc_data = excel_data[['IDOC_Message_Type', 'Langtext_EN']].to_dict(orient='records')


class Stream:
    def __init__(self, signal_emitter):
        self.signal_emitter = signal_emitter

    def write(self, message):
        if message.strip():  # Ignore empty messages
            self.signal_emitter.text_signal.emit(message)

    def flush(self):
        pass


class Worker(QThread):
    def __init__(self, auth_url, request_url, api_token, xml_directory, output_base_dir, tag_name,
                 signal_emitter, create_factsheet, delta_operation, delete_operation, test_mode, import_dialog):
        super().__init__()
        self.auth_url = auth_url
        self.request_url = request_url
        self.api_token = api_token
        self.xml_directory = xml_directory
        self.output_base_dir = output_base_dir
        self.tag_name = tag_name
        self.second_tag_name = None  # Initialize the second tag name variable
        self.signal_emitter = signal_emitter
        self.create_factsheet = create_factsheet
        self.delta_operation = delta_operation
        self.delete_operation = delete_operation
        self.test_mode = test_mode
        self.import_dialog = import_dialog  # Reference to the ImportDialog instance
        self.test_file_path = None
        self._is_running = True

    def run(self):
        sys.stdout = Stream(self.signal_emitter)
        sys.stderr = Stream(self.signal_emitter)

        try:
            if not os.path.exists(self.xml_directory):
                self.signal_emitter.no_files_found.emit()
                return

            xml_files = [file for file in os.listdir(self.xml_directory) if file.endswith('.xml')]
            if not xml_files:
                self.signal_emitter.no_files_found.emit()
                return

            required_files = ["ALE.xml", "Clients.xml", "Components.xml", "Hosts.xml", "Modules.xml", "RFC.xml",
                              "Systems.xml"]
            missing_files = [file for file in required_files if
                             not os.path.exists(os.path.join(self.xml_directory, file))]
            if missing_files:
                self.signal_emitter.file_check_failed.emit(", ".join(missing_files))
                return

            for i in range(101):
                if not self._is_running:
                    self.signal_emitter.progress_signal.emit(0)
                    return
                self.signal_emitter.progress_signal.emit(i)
                time.sleep(0.05)

            while True:
                try:
                    output_dir = process_system_analysis(self.auth_url, self.request_url, self.api_token,
                                                         self.xml_directory, self.output_base_dir, self.tag_name,
                                                         self.create_factsheet, self.delta_operation, self.test_mode,
                                                         self.delete_operation, self.second_tag_name)
                    self.signal_emitter.process_finished.emit()
                    self.test_file_path = output_dir
                    break

                except requests.RequestException as e:
                    if '401' in str(e) or '403' in str(e):
                        print("Authentication error detected.")
                        if not self.retry_reauthentication():
                            self.signal_emitter.auth_failed.emit()
                            break
                    else:
                        raise

        except requests.ConnectionError:
            self.signal_emitter.connection_error.emit()
        except requests.RequestException:
            self.signal_emitter.auth_failed.emit()
        except Exception as e:
            self.signal_emitter.unexpected_error.emit(f"An unexpected error occurred: {str(e)}")
        finally:
            self.signal_emitter.progress_signal.emit(0)
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__

    def retry_reauthentication(self):
        retry_count = 0
        max_retries = 5
        backoff_factor = 1

        while retry_count < max_retries:
            try:
                print(f"Retrying operation... (Attempt {retry_count + 1})")

                # Instead of reauthenticating, just wait and retry the operation
                time.sleep(backoff_factor ** retry_count)
                return True  # Simply indicate to retry the operation
            except Exception as e:
                print(f"Retry attempt failed: {e}")
                retry_count += 1

        print("Operation failed after maximum retry attempts.")
        return False

    def reauthenticate(self):
        try:
            response = requests.post(self.auth_url, data={'grant_type': 'client_credentials'},
                                     headers={'Authorization': f'Bearer {self.api_token}'})
            if response.status_code == 200:
                new_token = response.json().get('access_token')
                if new_token:
                    self.api_token = new_token  # Update the API token with the new one

                    # Update the ImportDialog with the new authentication data
                    self.import_dialog.api_token_input.setText(self.api_token)
                    self.import_dialog.host_name_input.setText(self.auth_url)
                    self.import_dialog.directory_input.setText(self.xml_directory)
                    self.import_dialog.tag_name_input.setText(self.tag_name)
                    self.import_dialog.second_tag_name_input.setText(self.second_tag_name)

                    return True
                else:
                    print("No access token found in the response.")
            else:
                print(f"Reauthentication failed with status code: {response.status_code}")
                print(f"Response content: {response.content}")
            return False
        except requests.RequestException as e:
            print(f"Reauthentication error: {str(e)}")
            return False

    def stop(self):
        self._is_running = False
        self.wait(5000)
        if self is not None:
            if self.isRunning():
                self.terminate()


class ImportDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Import Data")
        self.setStyleSheet("background-color: #1F1F1F; color: #E1E1E1;")
        self.setGeometry(100, 100, 800, 900)

        self.signal_emitter = SignalEmitter()
        self.signal_emitter.text_signal.connect(self.update_status)
        self.signal_emitter.process_finished.connect(self.on_process_finished)
        self.signal_emitter.auth_failed.connect(self.on_auth_failed)
        self.signal_emitter.no_files_found.connect(self.on_no_files_found)
        self.signal_emitter.file_check_failed.connect(self.on_file_check_failed)
        self.signal_emitter.unexpected_error.connect(self.on_unexpected_error)
        self.signal_emitter.connection_error.connect(self.on_connection_error)  # Connect the signal

        self.worker = None

        layout = QVBoxLayout()
        title = QLabel("Import Data")
        title.setStyleSheet("font-size: 20px; font-weight: bold; margin-bottom: 20px;")
        layout.addWidget(title, alignment=Qt.AlignCenter)

        self.host_name_input, host_name_layout = self.create_input_field("Sub-domain Name:")
        layout.addLayout(host_name_layout)

        self.api_token_input, api_token_layout = self.create_input_field("API Token:")
        layout.addLayout(api_token_layout)

        self.directory_input, directory_layout = self.create_input_field("Directory Path:", with_button=True)
        layout.addLayout(directory_layout)

        self.tag_name_input, tag_name_layout = self.create_input_field("Initial Tag Name:")
        layout.addLayout(tag_name_layout)

        # Create the second tag name input field
        self.second_tag_name_input, second_tag_name_layout = self.create_input_field("Second Tag Name:")
        layout.addLayout(second_tag_name_layout)

        # Combine Tag Name input and radio buttons in a single vertical layout
        tag_and_operation_layout = QVBoxLayout()
        tag_and_operation_layout.addLayout(tag_name_layout)

        # Radio buttons
        operation_layout = QHBoxLayout()
        self.operation_label = QLabel("Operation:")
        self.operation_label.setStyleSheet("font-size: 16px; color: #E1E1E1; margin-right: 5px;")
        operation_layout.addWidget(self.operation_label)

        self.radio_create_factsheet = QRadioButton("Initial Import")
        self.radio_create_factsheet.setStyleSheet("color: #E1E1E1; padding: 5px;")
        self.radio_create_factsheet.setChecked(True)
        self.radio_create_factsheet.setToolTip("This operation creates new factsheets.")
        operation_layout.addWidget(self.radio_create_factsheet)

        self.radio_delta_operation = QRadioButton("Delta Import")
        self.radio_delta_operation.setStyleSheet("color: #E1E1E1; padding: 5px;")
        self.radio_delta_operation.setToolTip("This operation updates existing factsheets based on new data.")
        operation_layout.addWidget(self.radio_delta_operation)

        self.radio_delete_operation = QRadioButton("Delete Operation")
        self.radio_delete_operation.setStyleSheet("color: #E1E1E1; padding: 5px;")
        self.radio_delete_operation.setToolTip("This operation deletes existing factsheets.")
        operation_layout.addWidget(self.radio_delete_operation)

        self.operation_group = QButtonGroup()
        self.operation_group.addButton(self.radio_create_factsheet)
        self.operation_group.addButton(self.radio_delta_operation)
        self.operation_group.addButton(self.radio_delete_operation)

        tag_and_operation_layout.addLayout(operation_layout)
        layout.addLayout(tag_and_operation_layout)

        self.operation_group = QButtonGroup()
        self.operation_group.addButton(self.radio_create_factsheet)
        self.operation_group.addButton(self.radio_delta_operation)
        self.operation_group.addButton(self.radio_delete_operation)

        tag_and_operation_layout.addLayout(operation_layout)
        layout.addLayout(tag_and_operation_layout)

        # Test Mode Checkbox and Import Button Layout
        test_mode_import_layout = QHBoxLayout()

        self.test_mode_checkbox = QCheckBox("Test Mode ")
        self.test_mode_checkbox.setStyleSheet("font-size: 16px; color: #E1E1E1; margin-left: 200px; margin-top: 10px;")

        test_mode_import_layout.addWidget(self.test_mode_checkbox)

        self.import_button = QPushButton("Execute")
        self.import_button.setStyleSheet("""
                   background-color: #4CAF50;
                   color: #FFFFFF;
                   font-size: 16px;
                   padding: 10px;
                   border: none;
                   margin-top: 10px;
                   margin-left: 20px;
                   border-radius: 5px;
               """)
        self.import_button.clicked.connect(self.on_import_button_clicked)
        test_mode_import_layout.addWidget(self.import_button)

        test_mode_import_layout.addStretch(1)  # Add stretch to align the button and checkbox to the left

        layout.addLayout(test_mode_import_layout)  # Add the combined layout to the main layout

        self.comment_box = QTextEdit(self)
        self.comment_box.setReadOnly(True)
        self.comment_box.setStyleSheet("""
                   background-color: #292929;
                   color: #E1E1E1;
                   padding: 10px;
                   border: 1px solid #555555;
                   border-radius: 5px;
                   margin-top: 20px;
                   min-height: 300px;
               """)
        layout.addWidget(self.comment_box)

        self.setLayout(layout)
        self.load_settings()

    def on_connection_error(self):
        QMessageBox.warning(self, "Connection Error",
                            "Internet connection is disconnected. Please check your connection and try again.")
        self.worker = None
        self.reset_import_button()

    def create_input_field(self, label_text, with_button=False):
        layout = QHBoxLayout()
        label = QLabel(label_text)
        label.setStyleSheet("font-size: 16px; color: #E1E1E1; margin-bottom: 5px; min-width: 150px;")
        line_edit = QLineEdit()
        line_edit.setStyleSheet("""
                   background-color: #292929;
                   color: #E1E1E1;
                   padding: 10px;
                   border: 1px solid #555555;
                   border-radius: 5px;
               """)
        line_edit.setFixedWidth(500)

        layout.addWidget(label)
        layout.addWidget(line_edit)

        info_icon = QLabel()
        info_icon.setPixmap(QPixmap("info_icon.png").scaled(20, 20, Qt.KeepAspectRatio, Qt.SmoothTransformation))
        info_icon.setCursor(Qt.PointingHandCursor)

        info_icon.setToolTip(self.get_tooltip_text(label_text))

        layout.addWidget(info_icon)

        if with_button:
            button = QPushButton("Browse")
            button.setStyleSheet("""
                       background-color: #444444;
                       color: #FFFFFF;
                       padding: 10px;
                       border: none;
                       border-radius: 5px;
                       margin-left: 10px;
                   """)
            button.clicked.connect(lambda: self.browse_directory(line_edit))
            layout.addWidget(button)

        layout.addStretch(1)
        return line_edit, layout

    def get_tooltip_text(self, label_text):
        tooltips = {
            "Sub-domain Name:": "Enter your subdomain name here e.g. https://{SUBDOMAIN}.leanix.net",
            "API Token:": "Enter your API token here, you can find it in the administration section under API token",
            "Directory Path:": "Enter your xml files folder path here",
            "Initial Tag Name:": "The initial tag name used in the operation",
            "Second Tag Name:": "The second tag name used in the operation"
        }
        return tooltips.get(label_text, "No information available.")

    def browse_directory(self, line_edit):
        directory = QFileDialog.getExistingDirectory(self, "Select Directory")
        if directory:
            line_edit.setText(directory)

    def on_import_button_clicked(self):
        if self.import_button.text() == "Execute":
            host_name = self.host_name_input.text()
            api_token = self.api_token_input.text()
            directory_path = self.directory_input.text()
            tag_name = self.tag_name_input.text()
            second_tag_name = self.second_tag_name_input.text()  # Get the second tag name
            output_base_dir = os.path.join(directory_path, "output_folder")
            test_mode = self.test_mode_checkbox.isChecked()

            create_factsheet = self.radio_create_factsheet.isChecked()
            delta_operation = self.radio_delta_operation.isChecked()
            delete_operation = self.radio_delete_operation.isChecked()

            if not host_name or not api_token or not directory_path or not tag_name or not second_tag_name:
                QMessageBox.warning(self, "Error", "All fields are required")
                return

            if not create_factsheet and not delta_operation and not delete_operation:
                QMessageBox.warning(self, "Error", "At least one operation must be selected")
                return

            auth_url = f'https://{host_name}.leanix.net/services/mtm/v1/oauth2/token'
            request_url = f'https://{host_name}.leanix.net/services/pathfinder/v1/graphql'

            self.worker = Worker(auth_url, request_url, api_token, directory_path, output_base_dir, tag_name,
                                 self.signal_emitter, create_factsheet, delta_operation, delete_operation, test_mode,
                                 self)
            self.worker.second_tag_name = second_tag_name  # Add this line if you need to use the second tag name
            self.worker.start()
            self.import_button.setText("Cancel")
            self.import_button.setStyleSheet("""
                       background-color: #F44336;
                       color: #FFFFFF;
                       font-size: 16px;
                       padding: 10px;
                       border: none;
                       margin-top: 10px;
                       margin-left: 20px;
                       border-radius: 5px;
                       """)
            self.import_button.clicked.disconnect()  # Disconnect the current slot
            self.import_button.clicked.connect(self.cancel_import)  # Connect to the cancel function
        else:
            self.cancel_import()

    # Modify this method inside the ImportDialog class
    def cancel_import(self):
        if self.worker and self.worker.isRunning():
            # Create a custom message box styled attractively
            msg_box = QMessageBox(self)
            msg_box.setIcon(QMessageBox.Question)
            msg_box.setWindowTitle("Cancel Operation")
            msg_box.setText("Do you want backup?")

            # Add buttons for Backup and Cancel
            backup_button = msg_box.addButton("Backup", QMessageBox.AcceptRole)
            cancel_button = msg_box.addButton("Cancel", QMessageBox.RejectRole)

            # Apply modern styling to the message box and buttons
            msg_box.setStyleSheet("""
                QMessageBox {
                    background-color: #2E2E2E;  /* Dark background */
                    border-radius: 10px;  /* Rounded corners */
                    padding: 15px;  /* Padding for spacing */
                    color: #FFFFFF;  /* White text */
                }
                QLabel {
                    color: #F5F5F5;  /* Light text color */
                    background-color: #2E2E2E;
                    font-size: 18px;  /* Slightly larger font size */
                    font-weight: bold;  /* Bold text for emphasis */
                }
                QPushButton {
                    background-color: #444444;  /* Dark button color */
                    color: #FFFFFF;
                    padding: 10px 25px;
                    border-radius: 8px;  /* Rounded button corners */
                    font-size: 16px;
                    font-weight: bold;  /* Bold button text */
                    transition: background-color 0.3s, transform 0.3s;  /* Smooth transition for hover effects */
                    box-shadow: 0px 4px 8px rgba(0, 0, 0, 0.2);  /* Add subtle shadow for 3D effect */
                }
                QPushButton:hover {
                    background-color: #4CAF50;  /* Green background on hover */
                    transform: scale(1.05);  /* Slightly increase size on hover */
                }
                QPushButton:pressed {
                    background-color: #388E3C;  /* Darker green when pressed */
                    transform: scale(0.95);  /* Slightly shrink when pressed */
                }
            """)

            # Execute the message box and handle user input
            result = msg_box.exec_()

            if result == QMessageBox.AcceptRole:
                # Handle backup logic here if 'Backup' is clicked
                self.create_backup()

            # Stop the worker thread and reset the UI
            self.worker.stop()
            self.worker = None
            self.import_button.setText("Execute")
            self.import_button.setStyleSheet("""
                   background-color: #4CAF50;
                   color: #FFFFFF;
                   font-size: 16px;
                   padding: 10px;
                   border: none;
                   margin-top: 10px;
                   margin-left: 20px;
                   border-radius: 5px;
                   """)
            self.comment_box.append("Execute operation cancelled")
            self.reset_import_button()

    def create_backup(self):
        # Define the backup process logic here
        self.comment_box.append("Backup initiated.")

    def on_process_finished(self):
        if self.worker:
            test_mode_prefix = "Test Mode: " if self.worker.test_mode else ""

            if self.worker.create_factsheet:
                msg_box = QMessageBox(self)
                msg_box.setIcon(QMessageBox.Information)
                msg_box.setWindowTitle("Operation Successful")
                msg_box.setText(f"{test_mode_prefix}The initial import operation was successfully done.")

                msg_box.setStyleSheet("""
                    QMessageBox {
                        background-color: #2C2C2C;
                        color: #E1E1E1;
                    }
                    QLabel {
                        color: #E1E1E1;
                        background-color: #2C2C2C;  /* Ensure background matches overall background */
                        font-size: 16px;
                        font-weight: bold;
                    }
                    QPushButton {
                        background-color: #444444;
                        color: #FFFFFF;
                        padding: 10px 20px;
                        border: none;
                        border-radius: 5px;
                        font-size: 14px;
                    }
                    QPushButton:hover {
                        background-color: #4CAF50;
                    }
                    QPushButton:pressed {
                        background-color: #388E3C;
                    }
                """)

                msg_box.exec_()
            elif self.worker.delta_operation:
                msg_box = QMessageBox(self)
                msg_box.setIcon(QMessageBox.Information)
                msg_box.setWindowTitle("Operation Successful")
                msg_box.setText(f"{test_mode_prefix}The delta import operation was successfully done.")

                msg_box.setStyleSheet("""
                    QMessageBox {
                        background-color: #2C2C2C;
                        color: #E1E1E1;
                    }
                    QLabel {
                        color: #E1E1E1;
                        background-color: #2C2C2C;  /* Ensure background matches overall background */
                        font-size: 16px;
                        font-weight: bold;
                    }
                    QPushButton {
                        background-color: #444444;
                        color: #FFFFFF;
                        padding: 10px 20px;
                        border: none;
                        border-radius: 5px;
                        font-size: 14px;
                    }
                    QPushButton:hover {
                        background-color: #4CAF50;
                    }
                    QPushButton:pressed {
                        background-color: #388E3C;
                    }
                """)

                msg_box.exec_()
            elif self.worker.delete_operation:
                msg_box = QMessageBox(self)
                msg_box.setIcon(QMessageBox.Information)
                msg_box.setWindowTitle("Operation Successful")
                msg_box.setText(f"{test_mode_prefix}The delete operation was successfully done.")

                msg_box.setStyleSheet("""
                    QMessageBox {
                        background-color: #2C2C2C;
                        color: #E1E1E1;
                    }
                    QLabel {
                        color: #E1E1E1;
                        background-color: #2C2C2C;  /* Ensure background matches overall background */
                        font-size: 16px;
                        font-weight: bold;
                    }
                    QPushButton {
                        background-color: #444444;
                        color: #FFFFFF;
                        padding: 10px 20px;
                        border: none;
                        border-radius: 5px;
                        font-size: 14px;
                    }
                    QPushButton:hover {
                        background-color: #4CAF50;
                    }
                    QPushButton:pressed {
                        background-color: #388E3C;
                    }
                """)

                msg_box.exec_()

            if self.worker.test_file_path:
                self.display_test_mode_data(self.worker.test_file_path)
                self.prompt_open_or_download(self.worker.test_file_path)

            self.worker = None
            self.import_button.setText("Execute")
            self.import_button.setStyleSheet("""
                background-color: #4CAF50;
                color: #FFFFFF;
                font-size: 16px;
                padding: 10px;
                border: none;
                border-radius: 5px;
            """)
        self.reset_import_button()

    def update_status(self, message):
        self.comment_box.append(message)

    def reset_import_button(self):
        self.import_button.setText("Execute")
        self.import_button.setStyleSheet("""
            background-color: #4CAF50;
            color: #FFFFFF;
            font-size: 16px;
            padding: 10px;
            border: none;
            margin-top: 10px;
            margin-left: 20px;
            border-radius: 5px;
        """)
        self.import_button.clicked.disconnect()  # Disconnect the current slot
        self.import_button.clicked.connect(self.on_import_button_clicked)  # Reconnect to the execute function

    def on_auth_failed(self):
        QMessageBox.warning(self, "Authentication Failed", "Sub-domain Name or API token is incorrect.")
        self.worker = None
        self.reset_import_button()  # Reset the button here

    def on_no_files_found(self):
        QMessageBox.warning(self, "No Files Found", "No XML files found in the directory.")
        self.worker = None
        self.reset_import_button()  # Reset the button here

    def on_file_check_failed(self, missing_files):
        QMessageBox.warning(self, "Missing Files", f"The following required XML files are missing: {missing_files}")
        self.worker = None
        self.reset_import_button()  # Reset the button here

    def on_unexpected_error(self, message):
        QMessageBox.warning(self, "Unexpected Error", message)
        self.worker = None
        self.reset_import_button()  # Reset the button here

    def display_test_mode_data(self, directory_path):
        try:
            # Assuming the file name is consistent; adjust accordingly
            excel_file = os.path.join(directory_path, "deleted_factsheets.xlsx")
            if os.path.exists(excel_file):
                df = pd.read_excel(excel_file)
                self.comment_box.append("\nTest Mode Data:\n")
                self.comment_box.append(df.to_string())

        except Exception as e:
            self.comment_box.append(f"\nFailed to load test mode data: {str(e)}")

    def prompt_open_or_download(self, file_path):
        msg_box = QMessageBox(self)
        msg_box.setIcon(QMessageBox.Question)
        msg_box.setWindowTitle("File Action")
        msg_box.setText(f"Do you want to open or download {os.path.basename(file_path)}?")

        msg_box.setStyleSheet("""
            QMessageBox {
                background-color: #2C2C2C;
                color: #E1E1E1;
            }
            QLabel {
                color: #E1E1E1;
                background-color: #2C2C2C;  /* Ensure background matches overall background */
                font-size: 16px;
                font-weight: bold;
            }
            QPushButton {
                background-color: #444444;
                color: #FFFFFF;
                padding: 10px 20px;
                border: none;
                border-radius: 5px;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #4CAF50;
            }
            QPushButton:pressed {
                background-color: #388E3C;
            }
        """)

        open_button = msg_box.addButton("Open", QMessageBox.AcceptRole)
        download_button = msg_box.addButton("Download", QMessageBox.AcceptRole)
        cancel_button = msg_box.addButton(QMessageBox.Cancel)

        msg_box.exec_()

        if msg_box.clickedButton() == open_button:
            self.open_file_in_excel(file_path)
        elif msg_box.clickedButton() == download_button:
            self.download_file(file_path)

    def open_file_in_excel(self, file_path):
        try:
            if platform.system() == 'Windows':
                os.startfile(file_path)
            elif platform.system() == 'Darwin':  # macOS
                subprocess.call(('open', file_path))
            else:  # Linux
                subprocess.call(('xdg-open', file_path))
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Could not open file: {str(e)}")

    def download_file(self, file_path):
        downloads_folder = os.path.join(os.path.expanduser('~'), 'Downloads')
        timestamp = time.strftime("%Y%m%d%H%M%S")
        base_name = os.path.basename(file_path)
        save_path = os.path.join(downloads_folder, f"{timestamp}_{base_name}")

        try:
            shutil.copy(file_path, save_path)
            msg_box = QMessageBox(self)
            msg_box.setIcon(QMessageBox.Information)
            msg_box.setWindowTitle("Download Successful")
            msg_box.setText(f"File saved to {save_path}")

            msg_box.setStyleSheet("""
                QMessageBox {
                    background-color: #2C2C2C;
                    color: #E1E1E1;
                }
                QLabel {
                    color: #E1E1E1;
                    background-color: #2C2C2C;  /* Ensure background matches overall background */
                    font-size: 16px;
                    font-weight: bold;
                }
                QPushButton {
                    background-color: #444444;
                    color: #FFFFFF;
                    padding: 10px 20px;
                    border: none;
                    border-radius: 5px;
                    font-size: 14px;
                }
                QPushButton:hover {
                    background-color: #4CAF50;
                }
                QPushButton:pressed {
                    background-color: #388E3C;
                }
            """)

            msg_box.exec_()
        except Exception as e:
            msg_box = QMessageBox(self)
            msg_box.setIcon(QMessageBox.Critical)
            msg_box.setWindowTitle("Download Failed")
            msg_box.setText(f"Could not save file: {str(e)}")

            msg_box.setStyleSheet("""
                QMessageBox {
                    background-color: #2C2C2C;
                    color: #E1E1E1;
                }
                QLabel {
                    color: #E1E1E1;
                    background-color: #2C2C2C;  /* Ensure background matches overall background */
                    font-size: 16px;
                    font-weight: bold;
                }
                QPushButton {
                    background-color: #444444;
                    color: #FFFFFF;
                    padding: 10px 20px;
                    border: none;
                    border-radius: 5px;
                    font-size: 14px;
                }
                QPushButton:hover {
                    background-color: #4CAF50;
                }
                QPushButton:pressed {
                    background-color: #388E3C;
                }
            """)

            msg_box.exec_()

    def show_custom_message_box(self, title, message, icon):
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle(title)
        msg_box.setText(message)
        msg_box.setIcon(icon)
        msg_box.setStyleSheet("""
            QMessageBox {
                background-color: #FFFFFF;
            }
            QLabel {
                color: #000000;
                background-color: #FFFFFF;
            }
            QSpacerItem {
                background-color: #FFFFFF;
            }
            QPushButton {
                background-color: #F0F0F0;
                border: 1px solid #C0C0C0;
                padding: 500px;
                min-width: 80px;
            }
            QPushButton:hover {
                background-color: #E0E0E0;
            }
        """)

        msg_box.exec_()

    def load_settings(self):
        try:
            with open("settings.json", "r") as settings_file:
                settings = json.load(settings_file)
                self.host_name_input.setText(settings.get("host_name", ""))
                self.api_token_input.setText(settings.get("api_token", ""))
                self.tag_name_input.setText(settings.get("tag_name", ""))
                self.second_tag_name_input.setText(settings.get("second_tag_name", ""))  # Load second tag name
                self.directory_input.setText(settings.get("directory_path", ""))
        except FileNotFoundError:
            pass


import json


class SettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self.setStyleSheet("background-color: #1F1F1F; color: #E1E1E1;")
        self.setGeometry(100, 100, 800, 400)

        layout = QVBoxLayout()

        self.host_name_input, host_name_layout = self.create_input_field("Sub-domain Name:")
        layout.addLayout(host_name_layout)

        self.api_token_input, api_token_layout = self.create_input_field("API Token:")
        layout.addLayout(api_token_layout)

        self.tag_name_input, tag_name_layout = self.create_input_field("Tag Name:")
        layout.addLayout(tag_name_layout)

        self.directory_input, directory_layout = self.create_input_field("Directory Path:", with_button=True)
        layout.addLayout(directory_layout)

        self.save_button = QPushButton("Save")
        self.save_button.setStyleSheet("""
           background-color: #4CAF50;
           color: #FFFFFF;
           font-size: 16px;
           padding: 10px;
           border: none;
           border-radius: 5px;
       """)
        self.save_button.clicked.connect(self.save_settings)
        layout.addWidget(self.save_button, alignment=Qt.AlignCenter)

        self.setLayout(layout)
        self.load_settings()

    def create_input_field(self, label_text, with_button=False):
        layout = QHBoxLayout()
        label = QLabel(label_text)
        label.setStyleSheet("font-size: 16px; color: #E1E1E1; margin-bottom: 5px; min-width: 150px;")
        line_edit = QLineEdit()
        line_edit.setStyleSheet("""
                   background-color: #292929;
                   color: #E1E1E1;
                   padding: 5px;
                   border: 1px solid #555555;
                   border-radius: 5px;
                   font-size: 18px;  # <-- Increase this value to make the text larger
               """)
        line_edit.setFixedWidth(500)

        layout.addWidget(label)
        layout.addWidget(line_edit)

        if with_button:
            button = QPushButton("Browse")
            button.setStyleSheet("""
                       background-color: #444444;
                       color: #FFFFFF;
                       padding: 10px;
                       border: none;
                       border-radius: 5px;
                       margin-left: 10px;
                   """)
            button.clicked.connect(lambda: self.browse_directory(line_edit))
            layout.addWidget(button)

        layout.addStretch(1)
        return line_edit, layout

    def browse_directory(self, line_edit):
        directory = QFileDialog.getExistingDirectory(self, "Select Directory")
        if directory:
            line_edit.setText(directory)

    def save_settings(self):
        settings = {
            "host_name": self.host_name_input.text(),
            "api_token": self.api_token_input.text(),
            "tag_name": self.tag_name_input.text(),

            "directory_path": self.directory_input.text()
        }

        with open("settings.json", "w") as settings_file:
            json.dump(settings, settings_file)

        QMessageBox.information(self, "Settings Saved", "Settings have been saved successfully.")
        self.close()

    def load_settings(self):
        try:
            with open("settings.json", "r") as settings_file:
                settings = json.load(settings_file)
                self.host_name_input.setText(settings.get("host_name", ""))
                self.api_token_input.setText(settings.get("api_token", ""))
                self.tag_name_input.setText(settings.get("tag_name", ""))
                self.directory_input.setText(settings.get("directory_path", ""))
        except FileNotFoundError:
            pass


class LandscapeAnalyzer(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Landscape Analyzer")
        self.setGeometry(100, 100, 1200, 800)
        self.setStyleSheet("background-color: #1F1F1F;")

        self.initUI()

    def initUI(self):
        header = QFrame(self)
        header.setStyleSheet("background-color: #292929; padding: 5px;")
        header.setFixedHeight(120)

        header_layout = QVBoxLayout()
        title_layout = QHBoxLayout()

        header_title = QLabel("Landscape Analyzer")
        header_title.setStyleSheet("color: #E1E1E1; font-size: 20px; font-weight: bold; font-family: Arial;")
        title_layout.addWidget(header_title, alignment=Qt.AlignCenter)

        header_layout.addLayout(title_layout)

        control_layout = QHBoxLayout()
        control_layout.setContentsMargins(0, 0, 0, 0)
        control_layout.setSpacing(5)

        self.chart_dropdown = QComboBox()
        self.chart_dropdown.addItems(["Bar Chart", "Pie Chart", "Marimekko Chart", "Radial Column Chart"])
        self.chart_dropdown.currentIndexChanged.connect(self.update_chart)
        self.chart_dropdown.setStyleSheet("""
           QComboBox {
               background-color: #FFFFFF;
               color: #000000;
               padding: 5px;
               border: 1px solid #555555;
               border-radius: 5px;
               font-size: 16px;
           }
           QComboBox QAbstractItemView {
               background-color: #FFFFFF;
               color: #000000;
               selection-background-color: #444444;
           }
       """)
        self.chart_dropdown.setVisible(False)
        control_layout.addWidget(self.chart_dropdown)

        self.analysis_dropdown = QComboBox()
        self.analysis_dropdown.setStyleSheet("""
           QComboBox {
               background-color: #FFFFFF;
               color: #000000;
               padding: 5px;
               border: 1px solid #555555;
               border-radius: 5px;
               font-size: 16px;
           }
           QComboBox QAbstractItemView {
               background-color: #FFFFFF;
               color: #000000;
               selection-background-color: #444444;
           }
       """)
        self.analysis_dropdown.currentIndexChanged.connect(self.on_analysis_selected)
        self.analysis_dropdown.setVisible(False)
        control_layout.addWidget(self.analysis_dropdown)

        self.export_button = QPushButton("Export Chart")
        self.export_button.setStyleSheet("""
           background-color: #4CAF50;
           color: #FFFFFF;
           font-size: 13px;
           padding: 10px;
           border: none;
           border-radius: 5px;
       """)
        self.export_button.clicked.connect(self.export_chart)
        self.export_button.setVisible(False)
        control_layout.addWidget(self.export_button, alignment=Qt.AlignRight)

        header_layout.addLayout(control_layout)
        header.setLayout(header_layout)

        sidebar = QFrame(self)
        sidebar.setStyleSheet("background-color: #333333; padding-top: 10px;")
        sidebar.setFixedWidth(220)

        sidebar_layout = QVBoxLayout()

        buttons = [("Import Data", "📂"), ("Analysis", "📊"), ("History", "📝"), ("Settings", "⚙️"), ("About Us", "ℹ️")]
        self.button_functions = [self.import_data, self.show_analysis, self.show_reports, self.show_settings,
                                 self.show_about]

        self.sidebar_buttons = []

        for (btn_text, icon_text), function in zip(buttons, self.button_functions):
            button = QPushButton(f"{icon_text} {btn_text}")
            button.setStyleSheet("""
               background-color: #444444;
               color: #FFFFFF;
               font-size: 18px;
               text-align: left;
               padding: 15px;
               border: none;
               border-radius: 10px;
               margin-bottom: 10px;
           """)
            button.clicked.connect(function)
            sidebar_layout.addWidget(button)
            self.sidebar_buttons.append(button)

        sidebar.setLayout(sidebar_layout)

        self.main_content = QScrollArea()
        self.main_content.setStyleSheet("border: none;")
        self.main_content_widget = QWidget()
        self.main_layout = QVBoxLayout(self.main_content_widget)

        self.graphs_layout = QVBoxLayout()

        self.chart_view = QGraphicsView()
        self.chart_view.setStyleSheet("""
           background-color: #292929;
           border: 1px solid #555555;
           border-radius: 10px;
       """)
        self.graphs_layout.addWidget(self.chart_view)

        self.main_layout.addLayout(self.graphs_layout)
        self.main_content.setWidget(self.main_content_widget)
        self.main_content.setWidgetResizable(True)

        self.right_panel = QFrame(self)
        self.right_panel.setFixedWidth(300)
        self.right_panel.setStyleSheet("background-color: #1F1F1F; color: #E1E1E1;")
        self.right_panel.setLayout(QVBoxLayout())

        self.close_button = QPushButton("✖️")
        self.close_button.setStyleSheet("""
           background-color: transparent;
           color: #E1E1E1;
           font-size: 16px;
           border: none;
           padding: 5px;
       """)
        self.close_button.clicked.connect(self.hide_right_panel)

        close_button_layout = QHBoxLayout()
        close_button_layout.addWidget(self.close_button, alignment=Qt.AlignRight)
        self.right_panel.layout().addLayout(close_button_layout)

        self.right_panel_label = QLabel("Factsheet Details")
        self.right_panel_label.setStyleSheet("font-size: 18px; font-weight: bold; margin: 10px;")
        self.right_panel.layout().addWidget(self.right_panel_label)

        self.factsheet_list = QListWidget()
        self.right_panel.layout().addWidget(self.factsheet_list)

        footer = QFrame(self)
        footer.setStyleSheet("background-color: #292929; color: #FFFFFF; padding: 10px;")
        footer.setFixedHeight(40)

        layout = QVBoxLayout()
        layout.addWidget(header)
        layout.addWidget(footer)

        content_layout = QHBoxLayout()
        content_layout.addWidget(sidebar)
        content_layout.addWidget(self.main_content)
        content_layout.addWidget(self.right_panel)

        layout.addLayout(content_layout)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

        self.right_panel.setVisible(False)
        self.load_analysis_directories()

    def has_valid_data(self, directory):
        created_file = os.path.join(directory, 'created_factsheets.xlsx')
        updated_file = os.path.join(directory, 'updated_factsheets.xlsx')
        deleted_file = os.path.join(directory, 'deleted_factsheets.xlsx')

        try:
            if (os.path.exists(created_file) and not pd.read_excel(created_file).empty) or \
                    (os.path.exists(updated_file) and not pd.read_excel(updated_file).empty) or \
                    (os.path.exists(deleted_file) and not pd.read_excel(deleted_file).empty):
                return True
        except Exception as e:
            print(f"Error loading data from {directory}: {e}")

        return False

    def load_analysis_directories(self):
        base_dir = "path_to_output_directory"
        if os.path.exists(base_dir):
            directories = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]

            valid_directories = [d for d in directories if self.has_valid_data(os.path.join(base_dir, d))]

            self.analysis_dropdown.clear()  # Clear existing items
            self.analysis_dropdown.addItems(valid_directories)

    def add_new_analysis_directory(self, directory):
        self.analysis_dropdown.addItem(directory)

    def show_about(self):
        self.reset_sidebar_styles()
        self.sidebar_buttons[4].setStyleSheet("""
           background-color: #4CAF50;
           color: #FFFFFF;
           font-size: 18px;
           text-align: left;
           padding: 15px;
           border: none;
           border-radius: 10px;
           margin-bottom: 10px;
       """)
        self.chart_dropdown.setVisible(False)
        self.analysis_dropdown.setVisible(False)
        self.export_button.setVisible(False)
        self.clear_main_content()
        self.display_about_us()

    def display_about_us(self):
        about_text = """<h1>About Us</h1>
            <p>This application is designed to streamline the analysis and reporting of SAP landscape data. It processes raw, 
            unstructured data extracted from SAP systems and transforms it into structured formats that are compatible with 
            LeanIX, enabling efficient data importation. The tool automates the extraction, normalization, and conversion of 
            complex SAP landscape information.</p>
            <p>By ensuring data integrity and consistency, it enhances the accuracy of enterprise architecture management 
            in LeanIX, facilitating better decision-making and strategic planning.</p>
            <p>Version: 1.0</p>
            <p>Developed by: CTI Consulting GmbH</p>
            <p>Visit us: <a href='https://cti.consulting/' style='color: #FFFFFF;'>https://cti.consulting/</a></p>
            <p>Contact: <a href='mailto:info@cti-consulting.de' style='color: #FFFFFF;'>info@cti-consulting.de</a></p>
            """
        about_dialog = QMessageBox(self)
        about_dialog.setWindowTitle("About Us")
        about_dialog.setText(about_text)
        about_dialog.setStandardButtons(QMessageBox.Ok)

        # Style the OK button
        ok_button = about_dialog.button(QMessageBox.Ok)
        ok_button.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: #FFFFFF;
                padding: 10px 20px;
                border: none;
                border-radius: 10px;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
            QPushButton:pressed {
                background-color: #388E3C;
            }
        """)

        # Style the message box itself
        about_dialog.setStyleSheet("""
            QMessageBox {
                background-color: #1F1F1F;  /* Dark gray background color */
            }
            QLabel {
                color: #E1E1E1;  /* Light text color */
                background-color: #1F1F1F;  /* Ensure background matches overall background */
                font-size: 16px;
            }
        """)

        about_dialog.exec_()

    def show_analysis(self):
        self.reset_sidebar_styles()
        self.sidebar_buttons[1].setStyleSheet("""
           background-color: #4CAF50;
           color: #FFFFFF;
           font-size: 18px;
           text-align: left;
           padding: 15px;
           border: none;
           border-radius: 10px;
           margin-bottom: 10px;
       """)
        self.chart_dropdown.setVisible(True)
        self.analysis_dropdown.setVisible(True)
        self.export_button.setVisible(True)
        self.clear_main_content()
        selected_directory = self.analysis_dropdown.currentText()
        if selected_directory:
            base_dir = "path_to_output_directory"
            dir_path = os.path.join(base_dir, selected_directory)
            self.load_analysis_data(dir_path)
            self.update_chart()

    def on_analysis_selected(self):
        selected_directory = self.analysis_dropdown.currentText()
        if selected_directory:
            base_dir = "path_to_output_directory"
            dir_path = os.path.join(base_dir, selected_directory)
            self.load_analysis_data(dir_path)
            if self.sidebar_buttons[1].styleSheet().find("background-color: #4CAF50") != -1:
                self.update_chart()

    def import_data(self):
        self.reset_sidebar_styles()
        self.sidebar_buttons[0].setStyleSheet("""
           background-color: #4CAF50;
           color: #FFFFFF;
           font-size: 18px;
           text-align: left;
           padding: 15px;
           border: none;
           border-radius: 10px;
           margin-bottom: 10px;
       """)
        self.chart_dropdown.setVisible(False)
        self.analysis_dropdown.setVisible(False)
        self.export_button.setVisible(False)
        self.clear_main_content()
        self.show_import_dialog()

    def show_reports(self):
        self.reset_sidebar_styles()
        self.sidebar_buttons[2].setStyleSheet("""
           background-color: #4CAF50;
           color: #FFFFFF;
           font-size: 18px;
           text-align: left;
           padding: 15px;
           border: none;
           border-radius: 10px;
           margin-bottom: 10px;
       """)
        self.chart_dropdown.setVisible(True)
        self.analysis_dropdown.setVisible(False)
        self.export_button.setVisible(False)
        self.clear_main_content()
        self.display_reports()

    def display_reports(self):
        self.clear_main_content()

        main_content_layout = QVBoxLayout()
        upper_box = QFrame()
        upper_box.setStyleSheet("background-color: #292929; border: 1px solid #555555; border-radius: 10px;")
        upper_box.setMinimumHeight(300)
        upper_box_layout = QVBoxLayout(upper_box)

        base_dir = "path_to_output_directory"
        directories = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]

        if directories:
            reports_table = QTableWidget(len(directories), 4)  # 4 columns: Timestamp, Created, Updated, Deleted
            reports_table.setHorizontalHeaderLabels(
                ["Timestamp", "Created Factsheet", "Updated Factsheet", "Deleted Factsheet"])
            reports_table.setStyleSheet("QHeaderView::section { background-color: #444444; color: #FFFFFF; }")
            reports_table.horizontalHeader().setStretchLastSection(False)
            reports_table.verticalHeader().setVisible(False)

            # Set equal column widths
            total_width = reports_table.viewport().width()
            column_width = total_width // 4  # Dividing by the number of columns (4 in this case)
            for col in range(4):
                reports_table.setColumnWidth(col, column_width)

            for row, directory in enumerate(directories):
                # Timestamp column
                timestamp_item = QTableWidgetItem(directory)
                timestamp_item.setFlags(timestamp_item.flags() & ~Qt.ItemIsEditable)
                timestamp_item.setForeground(QBrush(QColor("#FFFFFF")))
                timestamp_item.setTextAlignment(Qt.AlignCenter)  # Center the text
                reports_table.setItem(row, 0, timestamp_item)

                # Created Factsheet column
                created_file = "created_factsheets.xlsx"
                created_item = QTableWidgetItem(
                    created_file if os.path.exists(os.path.join(base_dir, directory, created_file)) else "")
                created_item.setFlags(created_item.flags() & ~Qt.ItemIsEditable)
                created_item.setForeground(QBrush(QColor("#FFFFFF")))
                created_item.setTextAlignment(Qt.AlignCenter)  # Center the text
                reports_table.setItem(row, 1, created_item)

                # Updated Factsheet column
                updated_file = "updated_factsheets.xlsx"
                updated_item = QTableWidgetItem(
                    updated_file if os.path.exists(os.path.join(base_dir, directory, updated_file)) else "")
                updated_item.setFlags(updated_item.flags() & ~Qt.ItemIsEditable)
                updated_item.setForeground(QBrush(QColor("#FFFFFF")))
                updated_item.setTextAlignment(Qt.AlignCenter)  # Center the text
                reports_table.setItem(row, 2, updated_item)

                # Deleted Factsheet column
                deleted_file = "deleted_factsheets.xlsx"
                deleted_item = QTableWidgetItem(
                    deleted_file if os.path.exists(os.path.join(base_dir, directory, deleted_file)) else "")
                deleted_item.setFlags(deleted_item.flags() & ~Qt.ItemIsEditable)
                deleted_item.setForeground(QBrush(QColor("#FFFFFF")))
                deleted_item.setTextAlignment(Qt.AlignCenter)  # Center the text
                reports_table.setItem(row, 3, deleted_item)

            reports_table.cellClicked.connect(self.on_xlsx_file_clicked)
            self.reports_table = reports_table  # Save reference to access in the handler
            upper_box_layout.addWidget(reports_table)
        else:
            no_reports_label = QLabel("No reports found.")
            no_reports_label.setStyleSheet("color: #E1E1E1; font-size: 16px;")
            upper_box_layout.addWidget(no_reports_label)

        main_content_layout.addWidget(upper_box)
        self.main_layout.addLayout(main_content_layout)

    def on_xlsx_file_clicked(self, row, column):
        if column in [1, 2, 3]:  # Ensure the click is on one of the XLSX files columns
            directory = self.reports_table.item(row, 0).text()
            xlsx_file = self.reports_table.item(row, column).text()

            if xlsx_file:  # Proceed only if there's a file listed
                base_dir = "path_to_output_directory"
                file_path = os.path.join(base_dir, directory, xlsx_file)
                self.prompt_open_or_download(file_path)

    def prompt_open_or_download(self, file_path):
        msg_box = QMessageBox(self)
        msg_box.setIcon(QMessageBox.Question)
        msg_box.setWindowTitle("File Action")
        msg_box.setText(f"Do you want to open or download {os.path.basename(file_path)}?")

        msg_box.setStyleSheet("""
                    QMessageBox {
                        background-color: #2C2C2C;
                        color: #E1E1E1;
                    }
                    QLabel {
                        color: #E1E1E1;
                        background-color: #2C2C2C;  /* Ensure background matches overall background */
                        font-size: 16px;
                        font-weight: bold;
                    }
                    QPushButton {
                        background-color: #444444;
                        color: #FFFFFF;
                        padding: 10px 20px;
                        border: none;
                        border-radius: 5px;
                        font-size: 14px;
                    }
                    QPushButton:hover {
                        background-color: #4CAF50;
                    }
                    QPushButton:pressed {
                        background-color: #388E3C;
                    }
                """)

        open_button = msg_box.addButton("Open", QMessageBox.AcceptRole)
        download_button = msg_box.addButton("Download", QMessageBox.AcceptRole)
        cancel_button = msg_box.addButton(QMessageBox.Cancel)

        msg_box.exec_()

        if msg_box.clickedButton() == open_button:
            self.open_file_in_excel(file_path)
        elif msg_box.clickedButton() == download_button:
            self.download_file(file_path)

    def open_file_in_excel(self, file_path):
        try:
            if platform.system() == 'Windows':
                os.startfile(file_path)
            elif platform.system() == 'Darwin':  # macOS
                subprocess.call(('open', file_path))
            else:  # Linux
                subprocess.call(('xdg-open', file_path))
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Could not open file: {str(e)}")

    def download_file(self, file_path):
        downloads_folder = os.path.join(os.path.expanduser('~'), 'Downloads')
        timestamp = time.strftime("%Y%m%d%H%M%S")  # Get the current timestamp in the format YYYYMMDDHHMMSS
        base_name = os.path.basename(file_path)
        save_path = os.path.join(downloads_folder, f"{timestamp}_{base_name}")

        try:
            shutil.copy(file_path, save_path)
            msg_box = QMessageBox(self)
            msg_box.setIcon(QMessageBox.Information)
            msg_box.setWindowTitle("Download Successful")
            msg_box.setText(f"File saved to {save_path}")

            msg_box.setStyleSheet("""
                QMessageBox {
                    background-color: #2C2C2C;
                    color: #E1E1E1;
                }
                QLabel {
                    color: #E1E1E1;
                    background-color: #2C2C2C;  /* Ensure background matches overall background */
                    font-size: 16px;
                    font-weight: bold;
                }
                QPushButton {
                    background-color: #444444;
                    color: #FFFFFF;
                    padding: 10px 20px;
                    border: none;
                    border-radius: 5px;
                    font-size: 14px;
                }
                QPushButton:hover {
                    background-color: #4CAF50;
                }
                QPushButton:pressed {
                    background-color: #388E3C;
                }
            """)

            msg_box.exec_()
        except Exception as e:
            msg_box = QMessageBox(self)
            msg_box.setIcon(QMessageBox.Critical)
            msg_box.setWindowTitle("Download Failed")
            msg_box.setText(f"Could not save file: {str(e)}")

            msg_box.setStyleSheet("""
                QMessageBox {
                    background-color: #2C2C2C;
                    color: #E1E1E1;
                }
                QLabel {
                    color: #E1E1E1;
                    background-color: #2C2C2C;  /* Ensure background matches overall background */
                    font-size: 16px;
                    font-weight: bold;
                }
                QPushButton {
                    background-color: #444444;
                    color: #FFFFFF;
                    padding: 10px 20px;
                    border: none;
                    border-radius: 5px;
                    font-size: 14px;
                }
                QPushButton:hover {
                    background-color: #4CAF50;
                }
                QPushButton:pressed {
                    background-color: #388E3C;
                }
            """)

            msg_box.exec_()

    def show_custom_message_box(self, title, message, icon):
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle(title)
        msg_box.setText(message)
        msg_box.setIcon(icon)
        msg_box.setStyleSheet("""
           QMessageBox {
               background-color: #E0E0E0;
           }
           QLabel {
               color: #000000;
               background-color: #E0E0E0;
           }
           QSpacerItem {
               background-color: #E0E0E0;
           }
           QPushButton {
               background-color: #F0F0F0;
               border: 1px solid #C0C0C0;
               padding: 500px;
               min-width: 80px;
           }
           QPushButton:hover {
               background-color: #E0E0E0;
           }
       """)

        # This section adjusts the icon's appearance
        msg_box.setStyleSheet("""
           QMessageBox QLabel {
               background-color: #E0E0E0;
           }
           QMessageBox QIcon {
               background-color: #E0E0E0;
           }
       """)

        msg_box.exec_()

    def display_xlsx_files(self, item):
        self.clear_main_content()
        reports_layout = QVBoxLayout()

        directory = item.text()
        base_dir = "path_to_output_directory"
        dir_path = os.path.join(base_dir, directory)
        xlsx_files = [f for f in os.listdir(dir_path) if f.endswith('.xlsx')]

        if xlsx_files:
            xlsx_files_list_widget = QListWidget()
            for xlsx_file in xlsx_files:
                item = QListWidgetItem(xlsx_file)
                item.setData(Qt.UserRole, os.path.join(dir_path, xlsx_file))
                xlsx_files_list_widget.addItem(item)
            xlsx_files_list_widget.itemClicked.connect(self.open_xlsx_file)
            reports_layout.addWidget(xlsx_files_list_widget)
        else:
            no_xlsx_files_label = QLabel("No XLSX files found in this directory.")
            no_xlsx_files_label.setStyleSheet("color: #E1E1E1; font-size: 16px;")
            reports_layout.addWidget(no_xlsx_files_label)

        self.main_layout.addLayout(reports_layout)

    def open_xlsx_file(self, item):
        xlsx_file_path = item.data(Qt.UserRole)
        if os.path.exists(xlsx_file_path):
            df = pd.read_excel(xlsx_file_path)
            self.display_dataframe(df)
        else:
            QMessageBox.warning(self, "File Not Found", f"The file {item.text()} could not be found.")

    def display_dataframe(self, df):
        self.clear_main_content()
        text_edit = QTextEdit()
        text_edit.setReadOnly(True)
        text_edit.setPlainText(df.to_string())
        text_edit.setStyleSheet("""
           background-color: #292929;
           color: #E1E1E1;
           padding: 10px;
           border: 1px solid #555555;
           border-radius: 5px;
           margin-top: 20px;
       """)
        self.main_layout.addWidget(text_edit)

    def show_settings(self):
        self.reset_sidebar_styles()
        self.sidebar_buttons[3].setStyleSheet("""
           background-color: #4CAF50;
           color: #FFFFFF;
           font-size: 18px;
           text-align: left;
           padding: 15px;
           border: none;
           border-radius: 10px;
           margin-bottom: 10px;
       """)
        self.chart_dropdown.setVisible(False)
        self.analysis_dropdown.setVisible(False)
        self.export_button.setVisible(False)
        self.clear_main_content()  # This will clear the main content (graphs, tables, etc.)
        self.hide_right_panel()  # Optionally hide the right panel
        self.show_settings_dialog()  # Show the settings dialog

    def load_analysis_data(self, directory):
        # Initialize empty DataFrames for created, updated, and deleted data
        created_df = pd.DataFrame()
        updated_df = pd.DataFrame()
        deleted_df = pd.DataFrame()

        # Attempt to load each file, if it exists, without logging messages
        created_file_path = os.path.join(directory, 'created_factsheets.xlsx')
        if os.path.exists(created_file_path):
            created_df = pd.read_excel(created_file_path)

        updated_file_path = os.path.join(directory, 'updated_factsheets.xlsx')
        if os.path.exists(updated_file_path):
            updated_df = pd.read_excel(updated_file_path)

        deleted_file_path = os.path.join(directory, 'deleted_factsheets.xlsx')
        if os.path.exists(deleted_file_path):
            deleted_df = pd.read_excel(deleted_file_path)

        # Update class attributes with loaded data
        self.created_df = created_df
        self.updated_df = updated_df
        self.deleted_df = deleted_df

        # If all DataFrames are empty, show a message
        if created_df.empty and updated_df.empty and deleted_df.empty:
            QMessageBox.warning(self, "No Data Available", "No data available from the XLSX files.")

    def plot_bar_chart(self):
        figure = Figure()
        canvas = FigureCanvas(figure)
        ax = figure.add_subplot(111)

        categories = ['Application', 'ITComponent', 'DataObject', 'BusinessCapability', 'Interface',
                      'TechnicalStack']
        colors = {'Created': 'green', 'Updated': 'blue', 'Deleted': 'red'}

        # Initialize counts as zero for each category
        created_counts = {category: 0 for category in categories}
        updated_counts = {category: 0 for category in categories}
        deleted_counts = {category: 0 for category in categories}

        # Update counts only if the respective DataFrame is not empty
        if not self.created_df.empty:
            created_counts.update(self.created_df['FactSheet Type'].value_counts().to_dict())
        if not self.updated_df.empty:
            updated_counts.update(self.updated_df['FactSheet Type'].value_counts().to_dict())
        if not self.deleted_df.empty:
            deleted_counts.update(self.deleted_df['FactSheet Type'].value_counts().to_dict())

        bar_width = 0.25
        r1 = range(len(categories))
        r2 = [x + bar_width for x in r1]
        r3 = [x + bar_width for x in r2]

        # Plot bars only if data is available
        if any(created_counts.values()):
            created_bars = ax.bar(r1, [created_counts[category] for category in categories], color=colors['Created'],
                                  width=bar_width, edgecolor='grey', label='Created')
            self.bind_bar_click_event(created_bars, 'Created', categories)

        if any(updated_counts.values()):
            updated_bars = ax.bar(r2, [updated_counts[category] for category in categories], color=colors['Updated'],
                                  width=bar_width, edgecolor='grey', label='Updated')
            self.bind_bar_click_event(updated_bars, 'Updated', categories)

        if any(deleted_counts.values()):
            deleted_bars = ax.bar(r3, [deleted_counts[category] for category in categories], color=colors['Deleted'],
                                  width=bar_width, edgecolor='grey', label='Deleted')
            self.bind_bar_click_event(deleted_bars, 'Deleted', categories)

        ax.set_xticks([r + bar_width for r in range(len(categories))])
        ax.set_xticklabels(categories)
        ax.set_title('Factsheet Operations by Type')
        ax.set_ylabel('Count')
        ax.yaxis.get_major_locator().set_params(integer=True)
        ax.legend()

        self.replace_graph(canvas)

    def bind_bar_click_event(self, bars, status, categories):
        for bar, category in zip(bars, categories):
            bar.set_picker(True)
            bar.status = status
            bar.category = category

    def on_bar_click(self, event):
        bar = event.artist
        status = bar.status
        category = bar.category

        if status == 'Created':
            factsheets = self.created_df[self.created_df['FactSheet Type'] == category]['Name'].tolist()
        elif status == 'Updated':
            factsheets = self.updated_df[self.updated_df['FactSheet Type'] == category]['Name'].tolist()
        elif status == 'Deleted':
            factsheets = self.deleted_df[self.deleted_df['FactSheet Type'] == category]['Name'].tolist()

        self.show_right_panel(factsheets)

    def plot_pie_chart(self):
        figure = Figure(figsize=(15, 5))
        canvas = FigureCanvas(figure)

        categories = ['Application', 'ITComponent', 'DataObject', 'BusinessCapability', 'Interface',
                      'TechnicalStack']
        colors = {
            'Application': '#0F7EB5', 'ITComponent': '#D29270', 'DataObject': '#774FCC',
            'BusinessCapability': '#003399', 'Interface': '#02AFA4', 'TechnicalStack': '#A6566D'
        }

        # Initialize counts as zero for each category
        created_counts = {category: 0 for category in categories}
        updated_counts = {category: 0 for category in categories}
        deleted_counts = {category: 0 for category in categories}

        # Update counts only if the respective DataFrame is not empty
        if not self.created_df.empty:
            created_counts.update(self.created_df['FactSheet Type'].value_counts().to_dict())
        if not self.updated_df.empty:
            updated_counts.update(self.updated_df['FactSheet Type'].value_counts().to_dict())
        if not self.deleted_df.empty:
            deleted_counts.update(self.deleted_df['FactSheet Type'].value_counts().to_dict())

        def format_label(count, total):
            return f'{count}'

        if any(created_counts.values()):
            ax1 = figure.add_subplot(131)
            labels1 = [category for category in categories if created_counts[category] > 0]
            sizes1 = [created_counts[category] for category in labels1]
            colors1 = [colors[category] for category in labels1]
            pie1 = ax1.pie(sizes1, labels=labels1, colors=colors1,
                           autopct=lambda pct: format_label(int(round(pct / 100. * sum(sizes1))), sum(sizes1)),
                           startangle=140)
            ax1.set_title('Created Factsheets')
            self.bind_pie_click_event(pie1, 'Created')

        if any(updated_counts.values()):
            ax2 = figure.add_subplot(132)
            labels2 = [category for category in categories if updated_counts[category] > 0]
            sizes2 = [updated_counts[category] for category in labels2]
            colors2 = [colors[category] for category in labels2]
            pie2 = ax2.pie(sizes2, labels=labels2, colors=colors2,
                           autopct=lambda pct: format_label(int(round(pct / 100. * sum(sizes2))), sum(sizes2)),
                           startangle=140)
            ax2.set_title('Updated Factsheets')
            self.bind_pie_click_event(pie2, 'Updated')

        if any(deleted_counts.values()):
            ax3 = figure.add_subplot(133)
            labels3 = [category for category in categories if deleted_counts[category] > 0]
            sizes3 = [deleted_counts[category] for category in labels3]
            colors3 = [colors[category] for category in labels3]
            pie3 = ax3.pie(sizes3, labels=labels3, colors=colors3,
                           autopct=lambda pct: format_label(int(round(pct / 100. * sum(sizes3))), sum(sizes3)),
                           startangle=140)
            ax3.set_title('Deleted Factsheets')
            self.bind_pie_click_event(pie3, 'Deleted')

        self.replace_graph(canvas)

    def bind_pie_click_event(self, pie, status):
        for wedge in pie[0]:
            wedge.set_picker(True)
            wedge.status = status

    def on_pie_click(self, event):
        wedge = event.artist
        status = wedge.status
        label = wedge.get_label()

        if status == 'Created':
            factsheets = self.created_df[self.created_df['FactSheet Type'] == label]['Name'].tolist()
        elif status == 'Updated':
            factsheets = self.updated_df[self.updated_df['FactSheet Type'] == label]['Name'].tolist()
        elif status == 'Deleted':
            factsheets = self.deleted_df[self.deleted_df['FactSheet Type'] == label]['Name'].tolist()

        self.show_right_panel(factsheets)

    def plot_marimekko_chart(self):
        figure = Figure()
        canvas = FigureCanvas(figure)
        ax = figure.add_subplot(111)

        categories = ['Application', 'ITComponent', 'DataObject', 'BusinessCapability', 'Interface',
                      'TechnicalStack']
        colors = {'Created': '#4CAF50', 'Updated': '#2196F3', 'Deleted': '#F44336'}

        data = {
            'Created': {category: 0 for category in categories},
            'Updated': {category: 0 for category in categories},
            'Deleted': {category: 0 for category in categories},
        }

        if not self.created_df.empty:
            data['Created'].update(self.created_df['FactSheet Type'].value_counts().to_dict())
        if not self.updated_df.empty:
            data['Updated'].update(self.updated_df['FactSheet Type'].value_counts().to_dict())
        if not self.deleted_df.empty:
            data['Deleted'].update(self.deleted_df['FactSheet Type'].value_counts().to_dict())

        total_counts = {category: sum(data[status][category] for status in data) for category in categories}
        max_count = max(total_counts.values())

        normalized_data = {status: {category: data[status][category] / max_count for category in categories} for status
                           in data}

        left = np.zeros(len(categories))

        for status in data:
            heights = [normalized_data[status].get(category, 0) for category in categories]
            counts_for_status = [data[status].get(category, 0) for category in categories]
            bars = ax.barh(categories, heights, left=left, color=colors[status], label=status)

            for bar, count, category in zip(bars, counts_for_status, categories):
                width = bar.get_width()
                if width > 0:
                    ax.text(bar.get_x() + width / 2, bar.get_y() + bar.get_height() / 2,
                            str(count), ha='center', va='center', color='black')
                    bar.set_picker(True)
                    bar.status = status
                    bar.category = category

            left += heights

        ax.set_title('Marimekko Chart for Factsheet Types')
        ax.set_xlabel('Proportion')
        ax.set_ylabel('FactSheet Types')
        ax.set_xlim(0, 1)
        ax.set_xticks(np.linspace(0, 1, 11))
        ax.set_xticklabels([f'{int(x * 100)}%' for x in np.linspace(0, 1, 11)])

        for tick in ax.get_xticklabels():
            tick.set_color('white')

        ax.legend()
        self.replace_graph(canvas)

    def on_marimekko_click(self, event):
        bar = event.artist
        status = bar.status
        category = bar.category

        if status == 'Created':
            factsheets = self.created_df[self.created_df['FactSheet Type'] == category]['Name'].tolist()
        elif status == 'Updated':
            factsheets = self.updated_df[self.updated_df['FactSheet Type'] == category]['Name'].tolist()
        elif status == 'Deleted':
            factsheets = self.deleted_df[self.deleted_df['FactSheet Type'] == category]['Name'].tolist()

        self.show_right_panel(factsheets)

    def plot_radial_column_chart(self):
        categories = ['Application', 'ITComponent', 'DataObject', 'BusinessCapability', 'Interface',
                      'TechnicalStack']

        created_counts = {category: 0 for category in categories}
        updated_counts = {category: 0 for category in categories}
        deleted_counts = {category: 0 for category in categories}

        if not self.created_df.empty:
            created_counts.update(self.created_df['FactSheet Type'].value_counts().to_dict())
        if not self.updated_df.empty:
            updated_counts.update(self.updated_df['FactSheet Type'].value_counts().to_dict())
        if not self.deleted_df.empty:
            deleted_counts.update(self.deleted_df['FactSheet Type'].value_counts().to_dict())

        angles = np.linspace(0, 2 * np.pi, len(categories), endpoint=False).tolist()
        angles += angles[:1]

        values_created = [created_counts[cat] for cat in categories] + [created_counts[categories[0]]]
        values_updated = [updated_counts[cat] for cat in categories] + [updated_counts[categories[0]]]
        values_deleted = [deleted_counts[cat] for cat in categories] + [deleted_counts[categories[0]]]

        figure = Figure()
        canvas = FigureCanvas(figure)
        ax = figure.add_subplot(111, polar=True)

        colors = ['green', 'blue', 'red']

        if any(values_created):
            bars_created = ax.bar(angles, values_created, color=colors[0], alpha=0.75, width=0.5, label='Created')
            self.bind_radial_click_event(bars_created, 'Created', categories)

        if any(values_updated):
            bars_updated = ax.bar(angles, values_updated, color=colors[1], alpha=0.75, width=0.3, label='Updated')
            self.bind_radial_click_event(bars_updated, 'Updated', categories)

        if any(values_deleted):
            bars_deleted = ax.bar(angles, values_deleted, color=colors[2], alpha=0.75, width=0.1, label='Deleted')
            self.bind_radial_click_event(bars_deleted, 'Deleted', categories)

        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(categories)
        ax.tick_params(pad=30)
        # Set the maximum value based on the data
        max_value = int(max(max(values_created), max(values_updated), max(values_deleted)))

        # Set the y-axis ticks to have only 4 concentric circles with integer values
        ax.set_yticks([0, max_value // 3, 2 * max_value // 3, max_value])

        # Set the y-tick labels to be integers
        ax.set_yticklabels([str(i) for i in [0, max_value // 3, 2 * max_value // 3, max_value]])

        ax.legend(loc='upper right', bbox_to_anchor=(1.2, 1.0))

        self.replace_graph(canvas)

    def bind_radial_click_event(self, bars, status, categories):
        for bar, category in zip(bars, categories):
            bar.set_picker(True)
            bar.status = status
            bar.category = category

    def on_radial_click(self, event):
        bar = event.artist
        status = bar.status
        category = bar.category

        if status == 'Created':
            factsheets = self.created_df[self.created_df['FactSheet Type'] == category]['Name'].tolist()
        elif status == 'Updated':
            factsheets = self.updated_df[self.updated_df['FactSheet Type'] == category]['Name'].tolist()
        elif status == 'Deleted':
            factsheets = self.deleted_df[self.deleted_df['FactSheet Type'] == category]['Name'].tolist()

        self.show_right_panel(factsheets)

    def show_right_panel(self, factsheets):
        self.right_panel.setVisible(True)
        self.factsheet_list.clear()

        # Display the count of factsheets
        count_item = QListWidgetItem(f"Factsheet Count: {len(factsheets)}")
        count_item.setFlags(count_item.flags() & ~Qt.ItemIsSelectable)
        self.factsheet_list.addItem(count_item)

        # Display each factsheet name
        for factsheet in factsheets:
            item = QListWidgetItem(factsheet)
            self.factsheet_list.addItem(item)

    def update_chart(self):
        if not hasattr(self, 'created_df') or not hasattr(self, 'updated_df') or not hasattr(self, 'deleted_df'):
            return

        selected_chart = self.chart_dropdown.currentText()
        self.clear_graph()
        if selected_chart == "Bar Chart":
            self.plot_bar_chart()
        elif selected_chart == "Pie Chart":
            self.plot_pie_chart()
        elif selected_chart == "Marimekko Chart":
            self.plot_marimekko_chart()
        elif selected_chart == "Radial Column Chart":
            self.plot_radial_column_chart()

    def replace_graph(self, new_graph):
        self.chart_view.setParent(None)
        self.chart_view = new_graph
        self.graphs_layout.addWidget(self.chart_view)

        self.chart_view.figure.canvas.mpl_connect('pick_event', self.on_radial_click)
        self.chart_view.figure.canvas.mpl_connect('pick_event', self.on_bar_click)
        self.chart_view.figure.canvas.mpl_connect('pick_event', self.on_pie_click)
        self.chart_view.figure.canvas.mpl_connect('pick_event', self.on_marimekko_click)

    def clear_graph(self):
        for i in reversed(range(self.graphs_layout.count())):
            widget = self.graphs_layout.itemAt(i).widget()
            if widget:
                widget.setParent(None)

    def hide_right_panel(self):
        self.right_panel.setVisible(False)

    def mousePressEvent(self, event):
        if self.right_panel.isVisible() and not self.right_panel.geometry().contains(event.globalPos()):
            self.right_panel.setVisible(False)
        super().mousePressEvent(event)

    def show_import_dialog(self):
        dialog = ImportDialog(self)
        dialog.exec_()

    def show_settings_dialog(self):
        dialog = SettingsDialog(self)
        dialog.setModal(True)  # Make the dialog modal
        dialog.exec_()

    def reset_sidebar_styles(self):
        for button in self.sidebar_buttons:
            button.setStyleSheet("""
               background-color: #444444;
               color: #FFFFFF;
               font-size: 18px;
               text-align: left;
               padding: 15px;
               border: none;
               border-radius: 10px;
               margin-bottom: 10px;
           """)

    def clear_main_content(self):
        while self.main_layout.count():
            child = self.main_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
        self.main_layout.addLayout(self.graphs_layout)

    def export_chart(self):
        if not hasattr(self, 'chart_view'):
            QMessageBox.warning(self, "No Chart", "No chart to export.")
            return

        file_path, _ = QFileDialog.getSaveFileName(self, "Save Chart", "",
                                                   "PNG Files (*.png);;JPG Files (*.jpg);;PDF Files (*.pdf)")

        if file_path:
            file_extension = os.path.splitext(file_path)[1].lower()

            if file_extension in ['.png', '.jpg', '.pdf']:
                self.chart_view.figure.savefig(file_path, format=file_extension[1:])
                QMessageBox.information(self, "Export Successful",
                                        f"Chart successfully exported as {file_extension[1:].upper()}.")
            else:
                QMessageBox.warning(self, "Invalid Format", "Unsupported file format. Please choose PNG, JPG, or PDF.")


class SplashScreen(QSplashScreen):
    def __init__(self, pixmap, rotation_interval=1000):
        super().__init__(pixmap)
        self.rotation_angle = 0
        self.rotation_interval = rotation_interval
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.rotate_logo)
        self.elapsed_timer = QElapsedTimer()

        self.init_label = QLabel("Initializing...", self)
        self.init_label.setStyleSheet("color: white; font-size: 24px; margin-bottom: 10px;")
        self.init_label.setAlignment(Qt.AlignCenter)
        self.init_label.setGeometry(0, self.pixmap().height() - 50, self.pixmap().width(), 50)

    def showEvent(self, event):
        self.elapsed_timer.start()
        self.timer.start(self.rotation_interval)
        QTimer.singleShot(2000, self.update_text)
        QTimer.singleShot(10000, self.finish_splash)

    def rotate_logo(self):
        self.rotation_angle = (self.rotation_angle + 180) % 360
        transform = QTransform().rotate(self.rotation_angle)
        rotated_pixmap = self.pixmap().transformed(transform, Qt.SmoothTransformation)
        self.setPixmap(rotated_pixmap)
        if self.elapsed_timer.elapsed() >= 10000:
            self.timer.stop()
            self.close()

    def update_text(self):
        self.init_label.setText("Application Loading...")

    def finish_splash(self):
        self.timer.stop()
        self.close()
        self.main_window.show()


if __name__ == '__main__':
    app = QApplication(sys.argv)

    # Load the logo as a QIcon
    logo_icon = QIcon("logo.png")

    # Set the application icon
    app.setWindowIcon(logo_icon)
    app.setStyleSheet("""
            QToolTip {
                color: #000000; /* Black text color */
                background-color: #ffffff; /* White background */
                border: 1px solid #888888;
                padding: 5px;
                font-size: 14px;
            }
        """)

    # Get the directory where the executable is located
    current_dir = os.path.dirname(os.path.abspath(sys.executable)) if getattr(sys, 'frozen',
                                                                              False) else os.path.dirname(
        os.path.abspath(__file__))

    # Load the splash screen image
    splash_pixmap = QPixmap(os.path.join(current_dir, "logo.png")).scaled(480, 480, Qt.KeepAspectRatio)
    splash = SplashScreen(splash_pixmap)

    window = LandscapeAnalyzer()

    # Set the window icon for the main window
    window.setWindowIcon(logo_icon)

    # If you have a system tray icon, you can set it like this:
    tray_icon = QSystemTrayIcon(logo_icon, window)
    tray_icon.show()

    splash.main_window = window
    splash.show()

    sys.exit(app.exec_())
