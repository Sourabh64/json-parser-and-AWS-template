import time
import json
import smtplib
import requests
import configparser
import pandas as pd
from datetime import timedelta, datetime, date
from requests.auth import HTTPBasicAuth
from ldap3 import Server, Connection, SUBTREE, ALL_ATTRIBUTES, MODIFY_REPLACE


class AD:
    def __init__(self):
        self.data = None
        self.lwd_data = None
        self.lwd = None
        self.employee_data = []
        self.incomplete_data = []
        self.inactive_users = []
        self.successful_list = []
        self.failed_list = []
        self.completed_list = []
        self.incomplete_list = []
        config = configparser.ConfigParser()
        config.read('C:/Users/sourabh.kulkarni/PycharmProjects/prodAD/config.ini')
        self.ldap_host = config.get('LDAP', 'host')
        self.ldap_port = int(config.get('LDAP', 'port'))
        self.ldap_username = config.get('LDAP', 'username')
        self.ldap_password = config.get('LDAP', 'password')
        self.ldap_searchbase = config.get('LDAP', 'search_base')
        self.user_name = config.get('Darwin', 'username')
        self.password = config.get('Darwin', 'password')
        self.api_key = config.get('Darwin', 'deactivate_api_key')
        self.dataset_key = config.get('Darwin', 'deactivate_dataset_key')
        self.smtp_server = config.get('SMTP', 'host')
        self.smtp_port = int(config.get('SMTP', 'port'))
        self.sender = config.get('SMTP', 'username')
        self.server_password = config.get('SMTP', 'password')
        self.error_email = config.get('Email', 'error_email')
        self.email_list = config.get('Email', 'email_list').split(",")
        to_address = []
        for email in self.email_list:
            to_address.append("<"+email+">")
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

    def get_inactive_users(self):
        try:
            print("Fetching users to be deactivated")
            url = "https://payu.darwinbox.in/masterapi/employee"
            api_key = self.api_key
            datasetkey = self.dataset_key
            l_date = date.today()
            lwd = l_date - timedelta(days=1)
            self.lwd = lwd.strftime("%d-%m-%Y")
            # self.lwd = '6-03-2023'
            body = json.dumps({"api_key": api_key, "datasetKey": datasetkey, "last_modified": self.lwd})
            response = requests.get(url, auth=HTTPBasicAuth(self.user_name, self.password), data=body)
            if response.status_code == 200:
                result = response.json()
                if result["status"] == 1:
                    self.inactive_users = result["employee_data"]
                else:
                    self.inactive_users = {}
            else:
                raise Exception(
                    f"Darwin API has given response status code of <b>{response.status_code}</b> while fetching inactive users data")
        except Exception as e:
            to_address = "<"+self.error_email+">"
            subject = "Darwin API error while fetching inactive users data"
            body = f"""
            Hi all,<br>

            {str(e)}<br>

            Kindly look at the API for more details.<br>
            """
            message = self.message_creation(to_address, subject, body)
            to_address = [self.error_email]
            self.send_mail(message, to_address)

    def deactivate_users_ad(self):
        try:
            deactivated_list = []
            deactivate_failed_list = []
            for employee in self.inactive_users:
                emp = {}
                print(f"Deactivating {employee['full_name']} in AD")
                server = Server(self.ldap_host, port=self.ldap_port, use_ssl=True)
                conn = Connection(server, user=self.ldap_username, password=self.ldap_password)
                conn.bind()
                conn.search(
                    search_base=self.ldap_searchbase,
                    search_filter=f"(userPrincipalName={employee['company_email_id']})",
                    search_scope=SUBTREE,
                    attributes=ALL_ATTRIBUTES,
                    get_operational_attributes=True,
                    paged_size=1000
                )
                response = conn.response[0]
                if 'dn' in response:
                    user_dn = response['dn']
                    response = conn.modify(user_dn, {'userAccountControl': (MODIFY_REPLACE, [514])})
                    if response:
                        emp['full_name'] = employee['full_name']
                        emp['employee_id'] = employee['employee_id']
                        emp['company_email_id'] = employee['company_email_id']
                        emp['date_of_exit'] = employee['date_of_exit']
                        deactivated_list.append(emp)
                    else:
                        deactivate_failed_list.append(employee)
                conn.unbind()
            if not deactivated_list and not deactivate_failed_list:
                to_address = self.emails
                subject = "Offboarded employee data"
                body = f"""
                        Hi all,<br>
                        There were no employees to be offboarded today as fetched from Darwin.
                        <br><br>
                        Thanks and Regards,<br>
                        HRMS Portal
                        """
                message = self.message_creation(to_address, subject, body)
                to_address = self.email_list
                self.send_mail(message, to_address)
            if deactivated_list:
                df = pd.json_normalize(deactivated_list)
                to_address = self.emails
                subject = "Offboarded employee data"
                body = f"""
                           Hi all,<br>
                           The below accounts has been disabled:<br><br>
                           {df.to_html()}<br><br>
                           Thanks and Regards,<br>
                           HRMS Portal
                           """
                message = self.message_creation(to_address, subject, body)
                to_address = self.email_list
                self.send_mail(message, to_address)
            if deactivate_failed_list:
                another_df = pd.json_normalize(deactivate_failed_list)
                to_address = self.emails
                subject = "Offboarded employee failed list"
                body = f"""
                        Hi all,<br>
                        The below accounts were not disabled:<br><br>
                        {another_df.to_html()}<br>

                        <br>
                        Thanks and Regards,<br>
                        HRMS Portal
                        """
                message = self.message_creation(to_address, subject, body)
                to_address = self.email_list
                self.send_mail(message, to_address)
        except Exception as e:
            to_address = "<"+self.error_email+">"
            subject = "Error while deactivating users in AD"
            body = f"""
             Hi all,<br>

             {str(e)}<br>

             Kindly look at the AD for more details.<br>
             """
            message = self.message_creation(to_address, subject, body)
            to_address = [self.error_email]
            self.send_mail(message, to_address)


if __name__ == '__main__':
    start = time.time()
    ad = AD()
    # Check for inactive users from Darwin API
    ad.get_inactive_users()
    # Make inactive users inactive in AD
    ad.deactivate_users_ad()
    print(time.time() - start)

