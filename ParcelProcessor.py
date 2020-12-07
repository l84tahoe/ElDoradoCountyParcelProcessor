"""
ParcelProcessor.py
Ryan Malhoski, City of South Lake Tahoe

This python script was developed to compare your local current parcel dataset
to El Dorado County and if the El Dorado County data is newer, download an
extraction of the parcel dataset from the county of El Dorado, CA. It uses
the extraction GP service on El Dorado County's GIS viewer to download the
parcels as a FGDB, makes an in_memory feature class, and parses ownership info
into the parts owner name, address, city, state, zip, and country. Then will
truncate and load the data into a designated dataset.

This script needs a config file to be in the same directory as this script.

See ParcelProcessorConfigExample.txt for info about the config file.

This script uses Python 3.x and was designed to be used with the default ArcGIS Pro
python enivorment "arcgispro-py3" with no need for installing new libraries.
"""

import requests
import arcpy
import json
import time
from zipfile import ZipFile
from io import BytesIO
import os
import shutil
import sys
from arcpy import env
import re
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
import smtplib
import traceback
import datetime
import base64
from pytz import timezone
import pytz
import pathlib
import configparser

# Set up email server and send email.
def email_smtp(subject,body,emailRecipients,emailLoginName,emailSever,emailServerPort,emailLoginPassword):
    message = MIMEMultipart()
    message['Subject'] = subject
    content = MIMEText(body,'plain')
    message.attach(content)
    mailserver = smtplib.SMTP(emailSever,emailServerPort)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.login(emailLoginName, emailLoginPassword)
    mailserver.sendmail(emailLoginName,emailRecipients, message.as_string())
    mailserver.quit()
    return 1

# Set logging.
def setup_logging(log_filename,logging_level):
    log_format = "%(asctime)s %(levelname)-8s %(message)s"
    log_date_format = "%a, %d %b %Y %H:%M:%S"
    logging.basicConfig(filename=log_filename,
                        filemode='a',
                        level=logging_level,
                        format=log_format,
                        datefmt=log_date_format)

# Set up parcel update function.
def updateParcels(zipPath,sridOut,parcelsDestination,startTime,emailSever,emailServerPort,emailLoginName,emailLoginPassword,emailRecipients):

    # Check if zip from failed attempt still exists
    existingZip = pathlib.Path(zipPath + r"\zipfolder")
    if existingZip.exists():
        shutil.rmtree(zipPath + r"\zipfolder")
        logging.info('Previous zip folder deleted')

    # Set up the regex queries for the data.
    cityStateZipRegex = r'(.+?)\s([A-Z]{1,2})\s(?=\d)(.*)'
    poBoxRegex = r'([^x]+)\W(P\s*O BOX\W*[0-9]{1,6})'
    addressRegex = r'(\d{1,5}\D+.+)'
    canadaRegex = r'(.+?)\s([A-Z]{1,2})\s(CANADA)\s(.*)'
    brazilRegex = r'(.+?)\s(BRAZIL)\s(.*)'
    
    # Set up list for addresses with a country name in the mail_addr4 column.
    countriesList = ['japan']

    # Set up CSLT fields to add to FGDB.
    newFields = [['owner', 'TEXT', 'owner', 255],
    ['owner_address', 'TEXT', 'address', 100],
    ['owner_city', 'TEXT', 'owner_city', 50],
    ['owner_state', 'TEXT', 'owner_state', 25],
    ['owner_zip', 'TEXT', 'owner_zip', 50],
    ['owner_country', 'TEXT', 'owner_country', 50],
    ['prcl_id_join','TEXT','prcl_id_join', 50]]

    # Setup the params for the extraction GP tool. The boundary is a polygon the grabs the whole county.
    payload = {'f': 'json', 'env:outSR': str(sridOut), 'Layers_to_Clip': '["Parcels"]', 'Area_of_Interest': '{"geometryType":"esriGeometryPolygon","features":[{"geometry":{"rings":[[[-13490599.294393552,4646257.881632805],[-13490599.294393552,4735689.204726496],[-13336502.24537058,4735689.204726496],[-13336502.24537058,4646257.881632805],[-13490599.294393552,4646257.881632805]]],"spatialReference":{"wkid":102100}}}],"sr":{"wkid":102100}}', 'Feature_Format': 'File Geodatabase - GDB - .gdb'}

    # Make the request to the GP service.
    logging.info('Requesting parcels from EDC')
    job = requests.get(r"https://see-eldorado.edcgov.us/arcgis/rest/services/uGOTNETandEXTRACTS/geoservices/GPServer/Extract%20Data%20Task/submitJob",params=payload)
    jobJson = job.json()

    # Check to make sure the job was accepted and get the JobID.
    if 'jobId' in jobJson:
        jobID = jobJson['jobId']
        jobStatus = jobJson['jobStatus']
        jobURL = r"https://see-eldorado.edcgov.us/arcgis/rest/services/uGOTNETandEXTRACTS/geoservices/GPServer/Extract%20Data%20Task/jobs"
        if jobStatus == 'esriJobSubmitted' or jobStatus == 'esriJobExecuting':
            logging.info('EDC job submitted')

        # Check the status of the job, when done grab the resulting ZIP file link.
        while jobStatus == 'esriJobSubmitted' or jobStatus == 'esriJobExecuting':
            time.sleep(5)
            jobCheck = requests.get(jobURL+"/"+jobID+"?f=json")
            jobJson = jobCheck.json()
            if 'jobStatus' in jobJson:
                jobStatus = jobJson['jobStatus']
                if jobStatus == "esriJobSucceeded":
                    if 'results' in jobJson:
                        logging.info('EDC server job completed')
                        resultURL = jobJson['results']['Output_Zip_File']['paramUrl']
                        # Grab the ZIP link.
                        logging.info('Downloading ZIP from EDC')
                        jobResult = requests.get(jobURL+"/"+jobID+r"/"+resultURL+r"?f=json&returnType=data")
                if jobStatus == "esriJobFailed":
                    logging.error('EDC server job failure')
                    if 'messages' in jobJson:
                        logging.error(jobJson['messages'])
                    raise ValueError('EDC job failed!')

    # Get the ZIP file.
    parcelsZip = requests.get(jobResult.json()['value']['url'])
    logging.info('Downloaded ZIP from EDC')

    # Save the ZIP into memory.
    zipFile = ZipFile(BytesIO(parcelsZip.content))

    # Unzip the ZIP to the defined path.
    for each in zipFile.namelist():
        if not each.endswith('/'):
            root, name = os.path.split(each)
            directory = os.path.normpath(os.path.join(zipPath, root))
            if not os.path.isdir(directory):
                os.makedirs(directory)
            open(os.path.join(directory, name), 'wb').write(zipFile.read(each))
    logging.info('Unzipped files in ' + str(zipPath))

    # Setup env for parcel FGDB and set overwrite to true.
    env.workspace = zipPath + r"\zipfolder"
    arcpy.env.overwriteOutput = True
    in_features = "data.gdb\Parcels"

    # Slap the data into an in_memory FC for super speed.
    memoryFeat = r"in_memory/inMemoryFeatureClass"
    arcpy.CopyFeatures_management(in_features, memoryFeat)

    # Check to make sure the EDC data isn't blank. This happens every so often.
    if arcpy.GetCount_management(memoryFeat)[0] == '0':
        # Send email saying EDC data is blank.
        subject = "PRODUCTION ERROR - Automated email: EDC parcel data is blank"
        message = MIMEMultipart()
        message['Subject'] = subject
        body = "The parcel data from EDC is blank. Please investigate."
        logging.error("The parcel data from EDC is blank.")
        content = MIMEText(body,'plain')
        message.attach(content)
        email_smtp(subject,body,emailRecipients,emailLoginName,emailSever,emailServerPort,emailLoginPassword)
        logging.info('Email Sent')
        return

    # Get rid of the fake OBJECTID field as it trips the append later on.
    arcpy.DeleteField_management(memoryFeat,'OBJECTID')

    # Add new fields to FGDB.
    arcpy.management.AddFields(memoryFeat,newFields)

    # Do work.
    with arcpy.da.UpdateCursor(memoryFeat, ['owner_name','mail_addr1','mail_addr2','mail_addr3','mail_addr4','owner','owner_address','owner_city','owner_state','owner_zip','owner_country','prcl_id']) as cursor:
        logging.info('Parsing ownership and address info...')
        for row in cursor:
            logging.debug("Working on "+ row[11])
            # Start from mail_addr4 and work left.
            if row[4] != ' ':
                logging.debug("Working on MAIL_ADDR4")
                if row[4] != 'UNKNOWN' and row[4].lower() not in countriesList:
                    # Parse out city, state, and zip code and assign variables.
                    cityStateZip = re.search(cityStateZipRegex, str(row[4]))
                    if cityStateZip is not None:
                        city = cityStateZip.group(1)
                        state = cityStateZip.group(2)
                        zipCode = cityStateZip.group(3)
                        country = ''
                    else:
                        continue
                    # Check to see if address starts with PO Box and assign variable.
                    if str(row[3]).startswith('PO') or str(row[3]).startswith('P O'):
                        address = str(row[3])
                    elif "PO BOX" in str(row[3]) or "P O BOX" in str(row[3]) or "P.O. BOX" in str(row[3]):
                        address = str(row[3])

                    # Parse out address that doesn't have PO Box and assign variable.
                    else:
                        add = re.search(addressRegex,str(row[3]))
                        address = add.group(1)

                    # Assign owner variable.
                    owner = str(row[0])+' '+str(row[1])+' '+str(row[2])
                elif row[4].lower() in countriesList:
                    country = str(row[4])
                    state = str(row[3])
                    city = str(row[2])
                    address = str(row[1])
                    owner = str(row[0])
                    zipCode = ''
                else:
                    owner = str(row[0])
                    address = ''
                    city = ''
                    state = ''
                    zipCode = ''
                    country = ''

            # If mail_addr4 is "empty".
            elif row[3] != ' ':
                logging.debug("Working on MAIL_ADDR3")
                # Parse out city, state, and zip code.
                cityStateZip = re.search(cityStateZipRegex, str(row[3]))

                # Foreign addresses won't parse so assign country, owner, address, and city variables. Set state and zip to blanks.
                if cityStateZip is None:
                    country = str(row[3])
                    owner = str(row[0])
                    address = str(row[1])
                    city = str(row[2])
                    state = ''
                    zipCode = ''
                else:
                    country = ''
                    row2 = str(row[2])

                    # Sanitize rows that start with a space.
                    if str(row[2]).startswith(' '):
                        row2 = str(row[2])[1:]

                    # Parse out city, state, and zip code and assign variables.
                    city = cityStateZip.group(1)
                    state = cityStateZip.group(2)
                    zipCode = cityStateZip.group(3)

                    # Check to see if address starts with PO Box and assign variable.
                    if row2.startswith('PO') or row2.startswith('P O') or row2.startswith('P.O.'):
                        address = row2

                    # Sometimes there may be a word in front of PO Box and parse that out and assign variable.
                    elif "PO BOX" in row2 or "P O BOX" in row2 or row2.startswith('ONE ') or row2.startswith('TWO '):
                        address = row2
                    else:
                        # Parse out address that doesn't have PO Box and assign variable, sometimes there no address so set variable to None.
                        add = re.search(addressRegex,row2)
                        if add is None:
                            address = 'None'
                        else:
                            address = add.group(1)

                    # Assign owner variable.
                    owner = str(row[0])+' '+str(row[1])

            # Before moving to mail_addr2 must capture "blanks" and USA owned parcels and insert blanks.
            elif row[0] == 'UNITED STATES OF AMERICA':
                cityStateZip = re.search(cityStateZipRegex, str(row[2]))
                owner = str(row[0])
                address = str(row[1])
                if cityStateZip is None:
                    city = ''
                    state = ''
                    zipCode = ''
                else:
                    city = cityStateZip.group(1)
                    state = cityStateZip.group(2)
                    zipCode = cityStateZip.group(3)
                country = ''
            elif row[0] == ' ':
                owner = ''
                address = ''
                city = ''
                state = ''
                zipCode = ''
                country = ''
            elif row[1] == ' ':
                owner = str(row[0])
                address = ''
                city = ''
                state = ''
                zipCode = ''
                country = ''

            # Parce the rest of the address info.
            else:
                logging.debug("Working on MAIL_ADDR2")
                if str(row[2]) == ' ':
                    owner = str(row[0])
                    address = str(row[1])
                    city = ''
                    state = ''
                    zipCode = ''
                    country = ''
                else:
                    row2 = str(row[2])

                    # Parse out city, state, and zip code and assign variables.
                    cityStateZip = re.search(cityStateZipRegex, row2)

                    # if it can't parse it's a foreign address and assign country variable.
                    if cityStateZip is None:
                        if "CANADA" in row2:
                            cityStateZip = re.search(canadaRegex, row2)
                            city = cityStateZip.group(1)
                            state = cityStateZip.group(2)
                            zipCode = cityStateZip.group(4)
                            country = cityStateZip.group(3)
                        if "BRAZIL" in row2:
                            cityStateZip = re.search(brazilRegex, row2)
                            city = cityStateZip.group(1)
                            state = ''
                            zipCode = cityStateZip.group(3)
                            country = cityStateZip.group(2)
                    else:
                        row1 = str(row[1])
                        country = ''
                        city = cityStateZip.group(1)
                        state = cityStateZip.group(2)
                        zipCode = cityStateZip.group(3)

                        # Sanitize rows that start with a space.
                        if row1.startswith(' '):
                            row1 = row1[1:]

                        # Check to see if address starts with PO Box and assign variable.
                        if row1.startswith('PO') or row1.startswith('P.O.') or row1.startswith('P O') or row1.startswith('P  O'):
                            address = str(row[1])

                        # Sometimes there may be a word in front of PO Box and parse that out and assign variable.
                        elif "PO BOX" in row1 or "P O BOX" in row1:
                            poBox = re.search(poBoxRegex,row1)

                            # If it can't be parsed assign variable.
                            if poBox is None:
                                address = row1
                            else:
                                address = poBox.group(2)
                        else:
                            # Parse out address that doesn't have PO Box and assign variable, sometimes there no address so set variable to None.
                            add = re.search(addressRegex,row1)

                            # Have exception for addresses that spell out 'one' instead of '1'.
                            if add is None or row1.startswith('ONE'):
                                address = row1
                            else:
                                address = add.group(1)

                    # Set owner variable.
                    owner = str(row[0])

            # Set up columns for the UpdateRow function.
            row[5] = owner
            row[6] = address
            row[7] = city
            row[8] = state
            row[9] = zipCode
            row[10] = country

            # Update the row.
            cursor.updateRow(row)
    del cursor

    # Set up legacy APN ID.
    logging.info('Creating legacy parcel ID')
    with arcpy.da.UpdateCursor(memoryFeat,['prcl_id','prcl_id_join']) as cursor:
        for row in cursor:
            row[1] = row[0][:6]+row[0][-2:]
            cursor.updateRow(row)
    del cursor

    # Truncate parcels data and copy new parcels.
    arcpy.TruncateTable_management(parcelsDestination)
    logging.info('Old parcels truncated')
    arcpy.Append_management(memoryFeat,parcelsDestination,'NO_TEST')
    logging.info('New parcels copied')

    # Delete the extracted ZIP folder.
    shutil.rmtree(zipPath + r"\zipfolder")
    logging.info('Zip folder deleted')

    # Set up total runtime variables.
    runTime = datetime.datetime.now() - startTime
    seconds = runTime.total_seconds()
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    # Send email saying completed.
    subject = "PRODUCTION - Automated email: El Dorado County Parcels Successfully Updated"
    message = MIMEMultipart()
    message['Subject'] = subject
    body = f"Parcels were successfully updated. Total runtime: {hours:.0f} hours, {minutes:.0f} minutes, and {seconds:.0f} seconds."
    logging.info(body)
    content = MIMEText(body,'plain')
    message.attach(content)
    email_smtp(subject,body,emailRecipients,emailLoginName,emailSever,emailServerPort,emailLoginPassword)
    logging.info('Email Sent')

# Main function.
def main():
    # Open the config file.
    config = configparser.ConfigParser()
    config.read(os.path.join(sys.path[0], 'ParcelProcessorConfig.txt'))
    for section in config.sections():
        
        # Set up email variables
        emailRecipients = config.get(section, "emailRecipients")
        emailSever = config.get(section, "emailSever")
        emailServerPort = config.get(section, "emailServerPort")
        emailLoginName = config.get(section, "emailLoginName")
        emailLoginPassword = config.get(section, "emailLoginPassword")
        try:
            # Get time right now for logging and script duration.
            startTime = datetime.datetime.now()

            # Set up logging.
            logDate = startTime.strftime("%Y-%m-%d")
            logFileDirectory = config.get(section, "logFileDirectory")
            logging_level = config.get(section, "logLevel")
            logFilename = logFileDirectory + "\\" + logDate + '_EDC_ParcelLoad.log'
            setup_logging(logFilename, logging_level)
            logging.info('Starting Script')

            # Set zip path
            zipPath = config.get(section, "zipPath")

            # Set out SRID
            sridOut = config.get(section, "sridOut")

            # Set up local parcel feature class path
            parcelsDestination = config.get(section, "parcelsDestination")

            # Check EDC parcel service to see newest data date and compare it to local data date.
            edcParcelUpdateDate = requests.get(r'https://see-eldorado.edcgov.us/arcgis/rest/services/uGOTNETandEXTRACTS/parcels/MapServer/1/query?where=1%3D1&outStatistics=%5B%7B%22statisticType%22%3A%22max%22%2C%22onStatisticField%22%3A%22LandManagement.Parcel.PRCL_GEOMETRY.POLY_CREATE_DATE%22%2C%22outStatisticFieldName%22%3A%22date%22%7D%5D&f=pjson')
            edcParcelUpdateDate = edcParcelUpdateDate.json()
            edcLastUpdate = datetime.datetime.fromtimestamp(edcParcelUpdateDate['features'][0]['attributes']['date']/1000)
            tz = pytz.timezone('America/Los_Angeles')
            edcLastUpdate = tz.localize(edcLastUpdate, is_dst=None)
            with arcpy.da.SearchCursor(parcelsDestination,'poly_creat',where_clause=('poly_creat IS NOT NULL'),sql_clause=(None, 'ORDER BY poly_creat DESC')) as cursor:
                sltLastUpdate = next(cursor)[0]
            del cursor
            sltLastUpdate = sltLastUpdate.replace(tzinfo=timezone('UTC'))
            sltLastUpdate = sltLastUpdate.astimezone(pytz.timezone("America/Los_Angeles"))
            logging.info("EDC newest date: "+str(edcLastUpdate))
            logging.info("Local newest date: "+str(sltLastUpdate))
            if sltLastUpdate != edcLastUpdate:
                logging.info("Parcels need updating")
                updateParcels(zipPath,sridOut,parcelsDestination,startTime,emailSever,emailServerPort,emailLoginName,emailLoginPassword,emailRecipients)
            else:
                logging.info("Parcels up to date, stopping script")

        except:
            # Uh oh spaghetti Os.....Send error email.
            logging.error("There was an error")
            subject = "PRODUCTION - Automated email for error during parcel load"
            tb = sys.exc_info()[2]
            tbInfo = traceback.format_tb(tb)[0]
            errorInfo = str(sys.exc_info()[0])+" - "+str(sys.exc_info()[1])
            pymsg = f"Python Errors:\nTraceback Info:\n {tbInfo}\nError Info:\n{errorInfo}"
            logging.error(pymsg)
            body = "There was an error, check the log file"
            email_smtp(subject,body,emailRecipients,emailLoginName,emailSever,emailServerPort,emailLoginPassword)

# Start script.
if __name__ == '__main__':
    main()