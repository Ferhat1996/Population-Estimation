import multiprocessing
import requests
import os
import pandas as pd
import argparse
import threading

from bs4 import BeautifulSoup
from os import listdir

# Define input and output folder names
input_folder_name = "Input"
output_folder_name = "Output"
output_folder = os.path.join(os.getcwd(), "Output")


# Define a function to clean a string by removing unnecessary characters
def clean_string(var):
    var = var.replace("\r", "").replace("\n", "").replace("  ", "").strip()
    return var


# Define a function to create a folder if it doesn't exist
def create_folder_if_not_exists(folder_name):
    path = os.path.join(os.getcwd(), folder_name)
    if not os.path.exists(path):
        os.makedirs(path)


# Define a function to fetch data from a website and save it to a file
def get_pages(begin, end):
    # iterate over loc_ids from begin to end
    for loc_id in range(begin, end):

        # create the url for the current loc_id
        website_url = "https://theclergydatabase.org.uk/jsp/locations/DisplayLocation.jsp?locKey=" + str(loc_id)

        # print a message indicating that we're fetching data from the current url
        print("Fetching data from: " + website_url)

        # fetch the page content from the url
        page = requests.get(website_url)

        # if the page request was successful
        if page.status_code == 200:

            # create a new file with the loc_id in the filename and write the page content to it
            with open(os.path.join(os.getcwd(), input_folder_name) + '/file' + str(loc_id) + '.html', 'wb+') as f:
                f.write(page.content)


# Define a function to get all the file paths in the input folder
def get_files_in_input_folder():
    files = []
    input_folder_path = os.path.abspath(input_folder_name)
    for file in listdir(input_folder_path):
        if file.endswith('.html'):
            file_path = os.path.join(input_folder_path, file)
            files.append(file_path)
    return files


# Define a function to read the contents of files, process them, and write the output to the output folder
def process_files(absolute_file_paths):
    for f in absolute_file_paths:
        print("Reading input file from: " + f)
        with open(f, 'rb') as file:
            soup = BeautifulSoup(file.read(), 'html.parser')

            # Initialize variables to None
        county_value = None
        jurisdiction_value = None
        geographic_value = None

        # Find the ul tag with class 's2'
        ul_tag = soup.find('ul', class_='s2')
        if ul_tag:
            # Find all the labels inside the ul tag
            labels = ul_tag.find_all('label')
            # Loop over each label
            for label in labels:
                # If the label text contains 'County:'
                if 'County:' in label.text:
                    # Set county_value to the cleaned string of the next sibling or next element of the label
                    county_value = clean_string(label.next_sibling or label.next_element)
                    # If the label text contains '(Jurisdiction)'
                elif '(Jurisdiction)' in label.text:
                    # Set jurisdiction_value to the cleaned string of the next sibling or next element of the label
                    jurisdiction_value = clean_string(label.next_sibling or label.next_element)
                    # If the label text contains '(Geographic)'
                elif '(Geographic)' in label.text:
                    # Set geographic_value to the cleaned string of the next sibling or next element of the label
                    geographic_value = clean_string(label.next_sibling or label.next_element)
                else:
                    # If the label text doesn't match any of the above, break out of the loop
                    break
            # extract the filename without the extension
            filename = os.path.splitext(os.path.basename(f))[0].replace("file", "")
            data_file_name = 'Data' + filename + '.csv'
            location_file_name = 'Location' + filename + '.csv'

            t1 = soup.find_all('table')
            if len(t1) > 1:
                t2 = t1[1]
                df = pd.DataFrame(columns=['Names', 'County', 'Juristiction', 'Geographic', 'PersonID', 'Year', 'Type', 'Office'])

                if t2 and t2.tbody:
                    for row in t2.tbody.find_all('tr'):
                        # find all columns:
                        columns = row.find_all('td')
                        names = clean_string(columns[0].text)
                        c = columns[0].find('a', href=True)
                        if c.__str__() != 'None':
                            persid = c['href'].replace('../persons/CreatePersonFrames.jsp?PersonID=','')
                        else:
                            persid = '0'

                        year = clean_string(columns[1].text)
                        type = clean_string(columns[2].text)
                        office = clean_string(columns[3].text)
                        # create a row of data for the dataframe (County, Juristicion, Geographic could be null for some files)
                        data_row = {'County': county_value, 'Juristiction': jurisdiction_value, 'Geographic': geographic_value, 'Names': names, 'PersonID': persid, 'Year': year, 'Type': type, 'Office': office}
                        # append the row to the dataframe
                        df = df._append(data_row, ignore_index=True)

                data_file_path = os.path.join(output_folder, data_file_name)
                # write the dataframe to a csv file
                df.to_csv(data_file_path, index=False)
                print("Output data file is written to: " + data_file_path)

            l1 = soup.find('ul', {"class": "s2"})
            if l1:
                cols = []
                for row in l1.find_all('li'):
                    try:
                        cols.append(row.label.text.lower().replace("\xa0", "").replace("(", "_").replace(')', '').replace(":", ""))
                        last = row.label.text.lower().replace("\xa0", "").replace("(", "_").replace(')', '').replace(":", "")
                    except:
                        cols.append(last)
                cols.append("parish")
                # print(cols)
                df = pd.DataFrame(cols)

                data = []
                for row in l1.find_all('li'):
                    text = row.text.lower().replace("\xa0", "").replace("(", "_").replace(')', '') \
                        .replace(":", "").replace("\n", "").replace("\r", "")
                    for c in range(0, len(cols)):
                        text = text.replace(cols[c], "")
                    text = text.replace("  ", "")
                    data.append(text)
                l2 = soup.find('div', {'class': 'ph'})
                data.append(l2.text.replace("\n", "").replace("\r", "").lower())

                df2 = pd.DataFrame(data)
                frame = [df, df2]
                final = pd.concat(frame, axis=1)
                final = final.transpose()
                # Write Location file to output folder
                final.to_csv(os.path.join(output_folder, location_file_name), index=False)
                print("Output Location file is written to: " + filename)


def merge_files():
    data_files = []
    for f in os.listdir(output_folder):
        if f.startswith("Data") and f.endswith(".csv"):
            data_files.append(f)

    common_files = []
    for f in os.listdir(output_folder):
        if f.startswith("Location") and f.endswith(".csv"):
            number = f.split("Location")[1].split(".")[0]
            if "Data" + number + ".csv" in data_files:
                common_files.append(f)

    # common_files = ["Location2.csv"]
    # Process each file
    df_list = []
    for file in common_files:
        loc = file.replace("Location", "").replace(".csv", "")

        # Import data from location
        data_file = os.path.join(output_folder, file)
        try:
            df_data = pd.read_csv(data_file, delimiter=",", skiprows=1)
            df_data['cced_id'] = int(loc)

            # Import location from location
            loc_file = os.path.join(output_folder, f"Location{loc}.csv")
            df_loc = pd.read_csv(loc_file, delimiter=",", skiprows=1, usecols=[1])
            #df_loc.columns = ['parish']
            df_loc['cced_id'] = int(loc)

            # Merge dataframes
            df_merged = pd.merge(df_data, df_loc, on='cced_id', how='left')

            df_merged = df_merged.drop('diocese_jurisdiction_y', axis=1)
            df_merged = df_merged.rename(columns={'diocese_jurisdiction_x': 'Juristiction'})
            df_merged = df_merged.rename(columns={'diocese_geographic': 'Geographic'})

            # Convert columns to string
            # df_merged['Office'].astype(str)
            df_merged['Juristiction'].astype(str)
            df_merged['Geographic'].astype(str)
            df_merged['county'].astype(str)
            df_list.append(df_merged)
        except:
            print("Error in file(s) " + loc)

    df_final = pd.concat(df_list)
    df_final.to_csv("CCEd.csv", sep=";", index=False)



# In command line, run following commands in a sequence. You need to run first command only once.
# python3 main.py --fetch_data true
# python3 main.py --process_data true
if __name__ == "__main__":
    # create the parser object
    parser = argparse.ArgumentParser()

    # add the first command line option
    parser.add_argument('--fetch_data', type=bool, help='Fetch data from API/Website')

    # add the second command line option
    parser.add_argument('--process_data', type=bool, help='Process existing data in the local file system')

    # add the third command line option for merge
    parser.add_argument('--merge_data', type=bool, help='Merge files')

    # parse the command line arguments
    args = parser.parse_args()

    # access the command line options
    fetch_data = args.fetch_data
    process_data = args.process_data
    merge_data = args.merge_data

    if fetch_data:
        create_folder_if_not_exists(folder_name=input_folder_name)

        process_list = []
        # loop over loc_id values in increments of 500
        for i in range(0, 25001, 500):
            # create a new process with a target function of get_pages and args of the current loc_id range
            process = multiprocessing.Process(target=get_pages, args=(i, i+99))
            # add the process to the list of processes
            process_list.append(process)
            # start the process
            process.start()

        # loop over the list of processes and wait for each process to finish
        for process in process_list:
            process.join()

    if process_data:
        create_folder_if_not_exists(folder_name=output_folder_name)
        files = get_files_in_input_folder()

        # Define the number of threads to use
        num_threads = 8

        # Split the files into groups based on the number of threads
        files_per_thread = len(files) // num_threads
        file_groups = [files[i:i+files_per_thread] for i in range(0, len(files), files_per_thread)]

        # Create a new thread for each group of files
        threads = []
        for file_group in file_groups:
            thread = threading.Thread(target=process_files, args=(file_group,))
            threads.append(thread)
            thread.start()

            # Wait for all threads to complete
        for thread in threads:
            thread.join()

    if merge_data:
        merge_files()

