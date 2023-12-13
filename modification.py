from ldap3 import Server, Connection, SUBTREE, ALL_ATTRIBUTES, MODIFY_REPLACE
import time
import json
import smtplib
import requests
import configparser
import pandas as pd
from datetime import timedelta, datetime, date
from requests.auth import HTTPBasicAuth


class ADModify:
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

    def ldap_login(self):
        server = Server(self.ldap_host, port=self.ldap_port, use_ssl=True)
        conn = Connection(server, user=self.ldap_username, password=self.ldap_password)
        conn.bind()
        return conn

    def ldap_search(self, conn, mail):
        conn.search(
            search_base=self.ldap_search_base,
            search_filter=f"(userPrincipalName={mail})",
            search_scope=SUBTREE,
            attributes=ALL_ATTRIBUTES,
            get_operational_attributes=True,
            paged_size=1000)
        response = conn.response[0]
        return response

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

    def modified_user_data(self):

        try:
            url = "https://payu.darwinbox.in/masterapi/employee"
            api_key = self.activate_api_key
            datasetKey = self.activate_dataset_key
            last_modified = date.today()
            last_modified = last_modified.strftime("%d-%m-%Y")
            last_modified = "13-3-2023"
            print("Getting modified employee data from Darwin")
            body = json.dumps({"api_key": api_key, "datasetKey": datasetKey, "last_modified": last_modified})
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
            to_address = "<"+self.error_email+">"
            subject = "Darwin API error while fetching employee modified data"
            body = f"""
            Hi all, <br>
            {str(e)} <br>
            Kindly test the API credentials for more details.
            """
            message = self.message_creation(to_address, subject, body)
            to_address = [self.error_email]
            self.send_mail(message, to_address)

    def modify_ad_user(self):
        try:
            employee_modified = []
            for employee in self.data:
                emp = {}
                conn = self.ldap_login()
                employee_mail = employee["company_email_id"]
                manager_email = employee["direct_manager_email"]
                emp["company_email_id"] = employee_mail
                emp["full_name"] = employee['full_name']
                response = self.ldap_search(conn, employee_mail)
                manager_response = self.ldap_search(conn, manager_email)
                if 'dn' in manager_response:
                    manager_dn = manager_response['dn']
                if 'dn' in response:
                    user_dn = response['dn']
                    if 'attributes' in response:
                        user_details = response['attributes']
                        if 'department' in user_details and user_details['department'] != employee['department'].strip().split("(")[0]:
                            print(employee['full_name'], employee['department'].split("(")[0], user_details['department'])
                            conn.modify(user_dn, {'department': (MODIFY_REPLACE, [employee['department'].split("(")[0]])})
                            emp["old_department"] = user_details['department']
                            emp["new_department"] = employee['department'].split("(")[0]
                        if 'title' in user_details and user_details['title'] != employee['designation'].strip().split("(")[0]:
                            print(employee['full_name'], employee['designation'].split("(")[0], user_details['title'])
                            conn.modify(user_dn, {'title': (MODIFY_REPLACE, [employee['designation'].split("(")[0]])})
                            emp["old_title"] = user_details['title']
                            emp["new_title"] = employee['designation'].split("(")[0]
                        if 'telephoneNumber' in user_details and user_details['telephoneNumber'] != employee['personal_mobile_no'].strip().split("'")[-1] and employee['personal_mobile_no']:
                            print(employee['full_name'], employee['personal_mobile_no'], user_details['telephoneNumber'])
                            conn.modify(user_dn, {'telephoneNumber': (MODIFY_REPLACE, [employee['personal_mobile_no'].split("'")[-1]])})
                            emp["old_personal_mobile_no"] = user_details['telephoneNumber']
                            emp["new_personal_mobile_no"] = employee['personal_mobile_no'].split("'")[-1]
                        if 'manager' in user_details and user_details['manager'] != manager_dn:
                            print(employee['full_name'], manager_dn, user_details['manager'])
                            conn.modify(user_dn, {'manager': (MODIFY_REPLACE, [manager_dn])})
                            emp["old_manager"] = user_details['manager']
                            emp["new_manager"] = employee['direct_manager_email']
                employee_modified.append(emp)
            if employee_modified:
                modified_df = pd.json_normalize(employee_modified)
                subject = "Modified employee data"
                body = f"""
                           Hi all,<br><br>
                           Below mentioned employees data have been modified in AD.
                           <br><br>
                           {modified_df.to_html()} <br><br><br>
                           Thanks and Regards,<br>
                           HRMS Portal"""
                to_address = self.emails
                message = self.message_creation(to_address, subject, body)
                to_address = self.email_list
                self.send_mail(message, to_address)
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


if __name__ == '__main__':
    ad_modify = ADModify()
    ad_modify.modified_user_data()
    ad_modify.modify_ad_user()
