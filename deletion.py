import time
import json
import smtplib
import requests
import configparser
import pandas as pd
from datetime import datetime, date
from requests.auth import HTTPBasicAuth
from ldap3 import Server, Connection, SUBTREE, ALL_ATTRIBUTES, MODIFY_REPLACE


class DeleteEmp:
    def __init__(self):
        self.data = None
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
        self.delete_api_key = config.get('Darwin', 'delete_api_key')
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
            url = "https://payu.darwinbox.in/UpdateEmployeeDetails/getDeletedEmployees"
            api_key = self.delete_api_key
            date_of_activation = date.today()
            date_of_activation = date_of_activation.strftime("%d-%m-%Y")
            date_of_activation = "3-8-2022"
            print("Getting onboarding employee data from Darwin")
            body = json.dumps({"api_key": api_key, "last_modified": date_of_activation})
            response = requests.get(url, auth=HTTPBasicAuth(self.user_name, self.password), data=body)
            if response.status_code == 200:
                result = response.json()
                if result["status"] == 1:
                    self.data = result["output"]
                else:
                    self.data = {}
            else:
                raise Exception(
                    f"Darwin API has given error while fetching new joining employee data <br>response status code: <b>{response.status_code}</b> <br>error: <b>{response.text}</b>")
        except Exception as e:
            to_address = "<"+self.error_email+">"
            subject = "Darwin API error while fetching new joinee data"
            body = f"""
            Hi all, <br>
            {str(e)} <br>
            Kindly test the API credentials for more details.
            """
            message = self.message_creation(to_address, subject, body)
            to_address = [self.error_email]
            self.send_mail(message, to_address)

    def delete_emp_ad(self):
        try:
            if self.data:
                deleted_list = []
                for emp in self.data:
                    if emp[3] == self.entity:
                        emp_name = emp[1]
                        emp_name = "Manoj"
                        server = Server(self.ldap_host, port=self.ldap_port, use_ssl=True)
                        conn = Connection(server, user=self.ldap_username, password=self.ldap_password)
                        conn.bind()
                        conn.search(
                            search_base=self.ldap_search_base,
                            search_filter=f"(givenName={emp_name})",
                            search_scope=SUBTREE,
                            attributes=ALL_ATTRIBUTES,
                            get_operational_attributes=True,
                            paged_size=1000
                        )
                        response = conn.response
                        for resp in response:
                            if 'dn' in resp:
                                user_dn = resp['dn']
                                if 'attributes' in resp:
                                    user_details = resp['attributes']
                                    if 'userAccountControl' in user_details and user_details['userAccountControl'] == 514:
                                        conn.delete(user_dn)
                                        deleted_list.append(emp_name)
                if deleted_list:
                    to_address = self.emails
                    subject = "Deleted employee data"
                    body = f"""
                               Hi all,<br>
                               The below accounts has been deleted as the employees didn't show up:<br><br>
                               {deleted_list}<br><br>
                               Thanks and Regards,<br>
                               HRMS Portal
                               """
                    message = self.message_creation(to_address, subject, body)
                    to_address = self.email_list
                    self.send_mail(message, to_address)
        except Exception as e:
            to_address = "<"+self.error_email+">"
            subject = "Error while Deleting employees"
            body = f"""
                                    Hi all,<br>

                                    {str(e)}<br>

                                    Kindly look at the script for more details."""
            message = self.message_creation(to_address, subject, body)
            to_address = [self.error_email]
            self.send_mail(message, to_address)


if __name__ == '__main__':
    start = time.time()
    delete_emp = DeleteEmp()
    # Get details from Darwin API
    delete_emp.get_darwin_data()
    # Delete users from AD
    delete_emp.delete_emp_ad()
    print(time.time() - start)
