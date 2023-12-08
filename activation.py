import time
import json
import smtplib
import requests
import configparser
import pandas as pd
from datetime import datetime, date
from requests.auth import HTTPBasicAuth
from ldap3 import Server, Connection, SUBTREE, ALL_ATTRIBUTES, MODIFY_REPLACE


class Activate:
    def __init__(self):
        self.data = None
        self.completed_list = []
        self.incomplete_list = []
        config = configparser.ConfigParser()
        config.read('C:/Users/sourabh.kulkarni/PycharmProjects/prodAD/config.ini')
        self.entity = config.get('Entity', 'entity_name')
        self.entity_domain = config.get('Entity', 'entity_domain')
        self.ldap_host = config.get('LDAP', 'host')
        self.ldap_port = int(config.get('LDAP', 'port'))
        self.ldap_username = config.get('LDAP', 'username')
        self.ldap_password = config.get('LDAP', 'password')
        self.ldap_search_base = config.get('LDAP', 'search_base')
        self.user_name = config.get('Darwin', 'username')
        self.password = config.get('Darwin', 'password')
        self.activate_api_key = config.get('Darwin', 'activate_api_key')
        self.activate_dataset_key = config.get('Darwin', 'activate_dataset_key')
        self.darwin_update = config.get('Darwin', 'update_darwin')
        self.smtp_server = config.get('SMTP', 'host')
        self.smtp_port = int(config.get('SMTP', 'port'))
        self.sender = config.get('SMTP', 'username')
        self.server_password = config.get('SMTP', 'password')
        self.error_email = config.get('Email', 'error_email')
        self.email_list = config.get('Email', 'email_list').split(",")
        to_address = []
        for email in self.email_list:
            to_address.append("<" + email + ">")
        self.emails = ", ".join(to_address)

    def message_creation(self, to_address, subject, body):
        message = f"""From: HRMS Notification <HRMS.Notification@payu.in>\nTo:{to_address}
        MIME-Version: 1.0\nContent-type: text/html\nSubject: {subject}\n{body}"""
        return message

    def send_mail(self, message, to_address):
        message = message
        try:
            smtpserver = smtplib.SMTP(self.smtp_server, self.smtp_port)
            smtpserver.starttls()
            smtpserver.login(self.sender, self.server_password)
            smtpserver.sendmail(self.sender, to_address, message)
        except smtplib.SMTPException as e:
            print(e)

    def get_darwin_data(self):
        try:
            url = "https://payu.darwinbox.in/masterapi/employee"
            api_key = self.activate_api_key
            datasetKey = self.activate_dataset_key
            date_of_activation = date.today()
            date_of_activation = date_of_activation.strftime("%d-%m-%Y")
            # date_of_activation = "28-2-2023"
            print("Getting onboarding employee data from Darwin")
            body = json.dumps({"api_key": api_key, "datasetKey": datasetKey, "date_of_activation": date_of_activation})
            response = requests.get(url, auth=HTTPBasicAuth(self.user_name, self.password), data=body)
            if response.status_code == 200:
                result = response.json()
                if result["status"] == 1:
                    self.data = result["employee_data"]
                else:
                    self.data = {}
            else:
                raise Exception(
                    f"Darwin API has given error while fetching new joining employee data <br>response status code: <b>{response.status_code}</b> <br>error: <b>{response.text}</b>")
        except Exception as e:
            to_address = "<" + self.error_email + ">"
            subject = "Darwin API error while fetching new joinee data"
            body = f"""
            Hi all, <br>

            This error occured while fetching employee data to be activated.
            {str(e)} <br>
            Kindly test the API credentials for more details.
            """
            message = self.message_creation(to_address, subject, body)
            to_address = [self.error_email]
            self.send_mail(message, to_address)

    def update_activate(self):
        try:
            if self.data:
                for employee in self.data:
                    emp_id = employee['employee_id']
                    company_email_id = employee['company_email_id']
                    server = Server(self.ldap_host, port=self.ldap_port, use_ssl=True)
                    conn = Connection(server, user=self.ldap_username, password=self.ldap_password)
                    conn.bind()
                    conn.search(
                        search_base=self.ldap_search_base,
                        search_filter=f"(userPrincipalName={company_email_id})",
                        search_scope=SUBTREE,
                        attributes=ALL_ATTRIBUTES,
                        get_operational_attributes=True,
                        paged_size=1000
                    )
                    response = conn.response[0]
                    if 'dn' in response:
                        user_dn = response['dn']
                        # userpswd = "setsomethinghere"
                        # conn.extend.microsoft.modify_password(user_dn, userpswd)
                        conn.modify(user_dn, {'EmployeeID': [(MODIFY_REPLACE, [emp_id])],
                                              'description': [(MODIFY_REPLACE, [emp_id])],
                                              'userAccountControl': [(MODIFY_REPLACE, [512])]})
                        # conn.modify(user_dn, {'pwdLastSet': [(MODIFY_REPLACE, [0])]})
                        manager_email = employee['direct_manager_email']
                        hrbp_email = employee['hrbp_email_id']
                        to_address = "<" + manager_email + ">," + "<" + hrbp_email + ">"
                        subject = "New onboarded employee details"
                        body = f"""
                                     Hi all,<br><br>
                                     New Joining Employee - {employee['full_name']} email id has been created.<br>
                                     <br> The Employee's email ID is: {employee['company_email_id']}.
                                     <br>
                                     Thanks and Regards,<br>
                                     HRMS Portal
                                     """
                        message = self.message_creation(to_address, subject, body)
                        to_address = [manager_email, hrbp_email]
                        self.send_mail(message, to_address)
        except Exception as e:
            to_address = "<" + self.error_email + ">"
            subject = "Error while activating employees"
            body = f"""
                        Hi all,<br>

                        {str(e)}<br>

                        Kindly look at the script for more details."""
            message = self.message_creation(to_address, subject, body)
            to_address = [self.error_email]

            self.send_mail(message, to_address)

    def update_darwin(self):
        try:
            url = "https://payu.darwinbox.in/UpdateEmployeeDetails/update"
            api_key = self.darwin_update
            for employee in self.data:
                emp = {}
                email_id = employee['company_email_id']
                employee_id = employee['employee_id']
                body = json.dumps({"api_key": api_key, "email_id": email_id, "employee_id": employee_id})
                response = requests.post(url, auth=HTTPBasicAuth(self.user_name, self.password), data=body)
                if response.status_code == 200:
                    emp['full_name'] = employee['company_email_id'].split("@")[0]
                    emp['email_id'] = employee['company_email_id']
                    manager_email = employee['direct_manager_email']
                    hrbp_email = employee['hrbp_email_id']
                    to_address = "<" + manager_email + ">," + "<" + hrbp_email + ">"
                    subject = "New onboarded employee"
                    body = f"""
                            Hi all,<br><br>
                            New Joining Employee - {emp['full_name']} email id has been created.<br>
                            <br> The Employee's email ID is: {emp['email_id']}.
                            <br>
                            Thanks and Regards,<br>
                            HRMS Portal
                            """
                    message = self.message_creation(to_address, subject, body)
                    to_address = [manager_email, hrbp_email]
                    self.send_mail(message, to_address)
                    self.completed_list.append(employee)
                else:
                    self.incomplete_list.append(employee)
            if self.incomplete_list:
                darwin_fail_df = pd.json_normalize(self.incomplete_list)
                subject = "Employees that are not updated to Darwin"
                body = f"""
                Hi all,<br>
                Below is the list of users whose email ID is not updated to Darwin.<br><br>
                {darwin_fail_df.to_html()} <br>
                <br> <br>
                <br><br><br>
                 Thanks and Regards,<br>
                 HRMS Portal
                """
                to_address = self.emails
                message = self.message_creation(to_address, subject, body)
                to_address = self.email_list
                self.send_mail(message, to_address)
            if not self.data:
                subject = "No employees to be updated to Darwin"
                body = """
                Hi all,<br><br>
                There are no employees fetched from Darwin to Onboard.
                <br><br>
                Thanks and Regards,<br>
                HRMS Portal"""
                to_address = self.emails
                message = self.message_creation(to_address, subject, body)
                to_address = self.email_list
                self.send_mail(message, to_address)
        except Exception as e:
            to_address = "<" + self.error_email + ">"
            subject = "Darwin API error while updating email ID to Darwin"
            body = f"""
            Hi all,<br>

            {str(e)}<br>
            Darwin failed to update the below mentioned employee list:<br>
            str({self.incomplete_list})<br>

            Kindly look at the API for more details.<br>
                    """
            message = self.message_creation(to_address, subject, body)
            to_address = [self.error_email]
            self.send_mail(message, to_address)


if __name__ == '__main__':
    start = time.time()
    activate = Activate()
    # Get details from Darwin API
    activate.get_darwin_data()
    # Validate the data if it is in the right format
    activate.update_activate()
    # Update Darwin with the email ID
    activate.update_darwin()
