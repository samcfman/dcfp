import datetime
import subprocess
import boto3
import pandas as pd
import os

now = datetime.datetime.now()
print ("START : Current date and time : ")
print (now.strftime("%Y-%m-%d %H:%M:%S"))

bucket_name = 'bank-input'

s3=boto3.client('s3', aws_access_key_id='', aws_secret_access_key='')

try:
 awsclicommand = 'cd ./splitted_files;aws s3 cp s3://bank-input/tr/tr_merge2.txt - | split -l 2000000 - tr_merge2'
 result = subprocess.run([awsclicommand], shell=True, capture_output=True, text=True)
except subprocess.CalledProcessError as e:
 raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


# TR FILES
# Define the field names and their respective widths for transaction files

transaction_field_names = ["A_Number", "B_Number", "Not_Used", "Exclude_from_Profiling", "Date_Time", "Transaction_Code",
 "Amount", "Debit_Credit", "Currency", "Original_Amount", "Reference", "Description", "Balance",
 "EOD_Balance", "B_Name", "B_Country", "Non_Accountholder", "Transaction_Field_1A", "Transaction_Field_1B",
 "Transaction_Field_2A", "Transaction_Field_2B", "Transaction_Field_3A", "Transaction_Field_3B",
 "Transaction_Field_4A", "Transaction_Field_4B", "Transaction_Field_4C", "Transaction_Field_5",
 "Transaction_Field_6"]
transaction_field_widths = [50, 48, 7, 1, 14, 50, 20, 1, 5, 20, 30, 160, 20, 20, 50, 2, 1, 30, 20, 30, 20, 30, 20, 5, 11, 34, 50, 50]


# assign directory
directory = './splitted_files'

# iterate over files in
# that directory
for filename in os.scandir(directory):
 if filename.is_file():
 print(filename.path)
 print(filename.name)

 df = pd.read_fwf(filename.path,
 header=None,widths=transaction_field_widths,
 names=transaction_field_names)
 # print("First 10 rows of the DataFrame:")
 # result = df.head(10)
 # print(result)

 csv_data = df.to_csv(index=False)
 outputfile = 'tr_output/' + filename.name
 s3.put_object(Body=csv_data, Bucket=bucket_name, Key=outputfile)

now = datetime.datetime.now()
print ("END : Current date and time : ")
print (now.strftime("%Y-%m-%d %H:%M:%S"))
