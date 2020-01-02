# -*- coding: utf-8 -*-
"""
INSIDEAIRBNB ALERTS

Created on Sat Dec  7 07:21:04 2019

@author: anguyen1210

This file contains additional tools used to create custom alerts for our work 
scraping the InsideAirBnb website. This file relies on the custom functions defined
in the 'insideairbnb_tools.py' file.
"""
import pandas as pd

        
# =============================================================================
# The following function uses the df returned from `extract_file_url` in the 
# 'insideairbnb_tools.py' file and checks it against the 'source_info' table in our 
# database to see if the file already exists in our database. The default of this 
# funtion is to return a logical, True/False if the file already exists. Changing 
# the default argument of this function, 'send_email' to True will cause the function
# to send an email alert with the results if True. 
# 
# The email parameters must be set manually in the `send_alert_email()` function below.
# =============================================================================

def check_import_list_for_new(import_list_df, send_email=False):
    """This function will send an email if new files are on the import list and 
    not already in our database.
    """
    #Fetch 'source_info' table from database to use for checks
    import sqlite3
    conn = sqlite3.connect("insideairbnb.db")
    source_info = pd.read_sql_query("select * from source_info;", conn)
    conn.close()

    #split the import list in order to match columns against 'source_info'
    from insideairbnb_tools import split_source_url    
    import_split = split_source_url(import_list_df)
    
    #test for new files in the splitted up import list
    test_for_new_files = pd.merge(import_split, source_info, on='source_url', how='left', indicator=True)
    
    if send_email:            
        if any(test_for_new_files['_merge'] == 'left_only'):
            #this block of codegives us a list of all the new city names
            new_cities = pd.merge(import_split, source_info, on='city', how='left', indicator=True)    
            new_cities = new_cities[new_cities['_merge'] == 'left_only'] 
            new_cities = new_cities.sort_values(['country_x', 'city'])
            new_cities = new_cities.city.unique()
            new_cities = pd.DataFrame(new_cities)
            new_cities.columns = ['cities_added']
            new_cities_names = new_cities.to_html() # we make a separate html object here and only use this if the 'new cities' object is greater thab zero
            
            #this block of code gives us a list of all the new files
            new_files = pd.merge(import_split, source_info, on=['city', 'source_url'], how='left', indicator=True)            
            new_files = new_files[new_files['_merge'] == 'left_only'] 
            new_files = new_files.sort_values(['country_x', 'source_url'])
            
            #this block of code takes the list of all new files and returns an html object with the count by country/city
            new_files_count = new_files.groupby(['country_x', 'city'])['source_url'].count()
            new_files_count = new_files_count.to_frame()
            new_files_count.columns = ['total_files_added']
            new_files_count = new_files_count.to_html() #html object to be inserted into email
            
            #this block of code takes the list of all new files and returns an html object with the entire list of new files
            new_files_all = new_files[['country_x', 'city', 'source_url']]
            new_files_all.columns = ['country', 'city', 'files_added']
            new_files_all = new_files_all.reset_index()
            new_files_all = new_files_all.to_html() #html object to be inserted in email
            
            if len(new_cities)==0:
                send_alert_email(new_files_count, new_files_all)
            else:
                send_alert_email(new_files_count, new_files_all, new_cities_names)          
        
        else:
            print('The import list supplied does not contain any new files')
    
    else:        
        #return boolean, if 'send_email' argument sent to False
        if any(test_for_new_files['_merge'] == 'left_only'):
            return True
        else:
            return False


# =============================================================================
# This function defines the template that will be used to send us alert emails 
# when there is new data available on the insideairbnb site for download. A simple
# text email template is defined, along with an html email that provides additional 
# details on all of the new files available, as well as any new cities added that 
# that is not alread in our database. Email recipient information must be set
# inside the function for the time being.        
# =============================================================================

def send_alert_email(new_files_count, new_files_all, new_cities_names=None):
    """This function defines the email templates that will be sent alerting us to
    new file uploads on the insideairbnb site"""
    
    import smtplib, ssl
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    sender_email = "sender@email.com"
    receiver_email = "receiver@email.com"
    #password = input("Type your password and press enter:")
    password = 'PASSWORD'
    
    message = MIMEMultipart("alternative")
    message["Subject"] = "New InsideAirBnB data available!"
    message["From"] = sender_email
    message["To"] = receiver_email
    
    # Create the plain-text and HTML version of your message
    text = """\
    YO!,
    There are new files available for download on the InsideAirBnB site.
    Have a look:
    http://insideairbnb.com/get-the-data.html"""
    
    if new_cities_names is None:
        html = """\
    <html>
      <body>
        <p>YO!<br><br>
            There are new files available for download on the InsideAirBnb site.
        </p>
        <p>Here are the number of new files per country/city:<br>
            {0}
        </p> 
        <p>Here are the new files added since the last update:<br>
            {1}
        </p>
      </body>
    </html>
    """.format(new_files_count, new_files_all)
    
    else:
        html = """\
    <html>
      <body>
        <p>YO!<br><br>
            There are new files available for download on the InsideAirBnb site.
        </p>
        <p>Here are the new cities added since the last update:<br>
            {2}
        </p> 
        <p>Here are the number of new files per country/city:<br>
            {0}
        </p> 
        <p>Here are the new files added since the last update:<br>
            {1}
        </p>
      </body>
    </html>
    """.format(new_files_count, new_files_all, new_cities_names)     
        
    # Turn these into plain/html MIMEText objects
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")
    
    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)
    message.attach(part2)
    
    # Create secure connection with server and send email
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(
                sender_email, receiver_email, message.as_string()
            )
            server.close() #maybe comment this out if it doesn't work? 
        print('Email sent!')       
             
    except Exception as e:
        print(e)
        print ('Email not sent. Something went wrong...')