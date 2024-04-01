import psycopg2
import smtplib
import os
import boto3
from botocore.exceptions import ClientError
import json

def lambda_handler(event,context):
    values=get_secret()
    desthost = values['host']
    dbname = values['engine']
    port = '5432'
    user = values['username']
    password = values['password']
   
    conn_string = "dbname='"+ dbname + "' port='" + port + "' user='"+ user +"' password='"+ password +"' host='" + desthost + "'"
    #print(conn_string)
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("SELECT status FROM pglogical.show_subscription_status()")
    # cursor.execute("SELECT status FROM pglogical.show_subscription_status(subscription_name := 'commhub_oh_sub') where status='replicating'")
    row = cursor.fetchone()
    conn.commit()
    cursor.close()
    #print(row[0] + " " + conn_string)
    #return row[0] + " " + conn_string

    if row[0] != 'replicating':
      id = os.environ['MONITOR_ID']   
      update_statement = "UPDATE dbo.monitor_emails " \
                          "SET updated_ts=CURRENT_TIMESTAMP " \
                          "WHERE monitor_id = %s " \
                          "AND (EXTRACT(EPOCH  FROM (CURRENT_TIMESTAMP - updated_ts))*1000 > email_send_interval_ms)"
    
      
      updated_rows = 0
    
      cursor2 = conn.cursor()
      cursor2.execute(update_statement, (id,))
    
      conn.commit()
      cursor2.close()
    
    
      updated_rows = cursor2.rowcount
    
      print("updated_rows = " + str(updated_rows))
      if updated_rows > 0:
        #return row
        # initialize variables
        host = os.environ['SMTPHOST']
        port = os.environ['SMTPPORT']
        sourcehost = os.environ['SOURCEHOST']
        mail_from = os.environ.get('MAIL_FROM')
        mail_to = os.environ['MAIL_TO']     # separate multiple recipient by comma. eg: "abc@gmail.com, xyz@gmail.com"
        #reply_to = event['queryStringParameters'].get('reply')
        #subject = event['queryStringParameters']['subject']
      
        subject = 'US IVR Alert! - Non-Prod - IVR primary database to alalytics postgres database replication is down in Ohio'

        #body =  conn_string + ' - replicating'
        body = "Hi Team, \n\n"\
        "    The IVR  primary Postgres database in Ohio to Analytics postgres database in Ohio replication is down, please take necessory action. \n\n"\
        "    Below are the origin and destinations instances.\n\n"\
        "    Origin as IVR primary database in Ohio : " + sourcehost + "\n\n"\
        "    Destination as analytics database in Ohio : " + desthost + "\n\n"\
        "    Note: Please do not reply to this email. \n\n"\
        "    Regards, \n\n"\
        "    Auto monitoring!"      

        #return body
        #send mail
        success = False
        success = send_email(host, subject, body, mail_to, mail_from)
        return success

# Fectching aws secret values here using below code 
   
def get_secret():

    #secret_name = "ivr-db-crediantials-nonprod"
    secret_name = "ivr-analytics-db-crediantials-nonprod"
    region_name = "us-east-2"
    session = boto3.session.Session()
    client = session.client(
    service_name='secretsmanager',
    region_name=region_name
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    #print(get_secret_value_response['SecretString'])
    values = json.loads(get_secret_value_response['SecretString'])
    return values
    
def send_email(host, subject, body, mail_to, mail_from = None, reply_to = None):
    if mail_from is None: mail_from = username
    if reply_to is None: reply_to = mail_to

    message = """From: %s\nTo: %s\nReply-To: %s\nSubject: %s\n\n%s""" % (mail_from, mail_to, reply_to, subject, body)
    print (message)
    try:
        server = smtplib.SMTP(host)
        server.ehlo()
        server.starttls()
        
        receivers = make_address_list(mail_to)
       
        emailStatus = server.sendmail(mail_from, receivers, message)
        print(emailStatus)
        server.close()
        return True
    except Exception as ex:
        print (ex)
        return False


def make_address_list (addresses):
    if isinstance(addresses, str):
        receivers = addresses.replace(' ','').split(',')
    elif isinstance(addresses, list):
        receivers = addresses
    return receivers