#!/usr/bin/env python
# coding: utf-8

# # BYOD -- Paternity Test
#  
# ## Goal
# Generates a Terra data table from files that a user has uploaded into a workspace bucket, where each row represents a "parent" file and has cells for its "children." 
# 
# It is named Paternity Test in the sense that parents and children are linked in the same table, where the parent is the file that links all the other related files together. What is determined to be a parent is set by the user specifying a file extension. For example, a CRAM file could be a parent, and its associated CRAI file could be considered its child. Another alternative is the parent could be a text file with information about the subject and the children could be multiple CRAM files from that subject.
# 
# ## Assumptions
# 1) You have a psuedofolder in your Terra data section that contains BYOD data in the form of files -- see [here](https://github.com/DataBiosphere/BYOD-to-Terra/blob/anvil/full_documentation.md) for info on that  
# 2) All of the files you want in your data table are in the same psuedofolder, and are not in sub-psuedofolders  
# 3) You've uploaded all your files already -- Terra data tables are not dynamic, so if you want to add more later, you'll have to re-run this notebook  
# 4) You are not trying to overwrite a data table you have already created. If you re-run this notebook and set TABLE_NAME to something that already exists as a Terra data table, the old table will NOT to overwritten. You will need to either pick a new name or delete the old table first.  
# 5) Your files follow a naming convention either like this...  
# `NWD119844.CRAM`  
# `NWD119844.CRAM.CRAI`  
# ...or this:  
# `NWD119844.CRAM`  
# `NWD119844.CRAI`  
# 
# You do NOT have to have run File Finder before this notebook. **The difference between this notebook and File Finder is that File Finder will create a row for every file in your psuedofolder, while Paternity Test will create a row for every parent file.**
# 
# Files that lack the parent file extension and are not children will NOT be added to the data table.
# 
# ## Results
# If you have one child per parent file, your output will look something like this.  
# 
# | parent | URI        | child1 | child1 URI |  
# |--------|------------|--------|------------|  
# | cram1  | gs://cram1 | crai1  | gs://crai1 |  
# | cram2  | gs://cram2 | crai2  | gs://crai2 |  
# | cram3  | gs://cram3 | crai3  | gs://crai3 |  
# 
# As more children are added, the number of rows will increase.  
# 
# | parent | URI        | child1 | child1 URI | child2 | child2 URI |  
# |--------|------------|--------|------------|--------|------------|  
# | cram1  | gs://cram1 | crai1  | gs://crai1 | txt1   | gs://txt1  |  
# | cram2  | gs://cram2 | crai2  | gs://crai2 | txt2   | gs://txt2  |  
# | cram3  | gs://cram3 | crai3  | gs://crai3 | txt3   | gs://txt3  |  

# ## Version history
# 
# | v | date | author | notes |
# | --- | --- | --- | --- |
# | 0.9 | April 6th 2020 | Ash | initial |
# | 1.0 | April 13th 2020 | Ash, Lon |   fixed multiple children and no parent usecases, implemented Lon's no-pandas code + Brian and Lon's other suggestions, better intro|
# | 1.1 | April 14th 2020 | Ash |   made Pandas code significantly faster (n=3000 files split over 1 parent and 2 children: 340 s --> 165 s)|
# | 1.2 | April 27th 2020 | Ash |   fixed NameError exception notice, rearranged for clarity and consistency across notebooks|
# | 1.3 | May 19th 2020 | Ash |   better logging, indeces now zfilled, parents now sort at front in panadas code, minor clarifications|
# | 1.4 | June 8th 2020 | Ash |   vastly improved pandas code, miscellaneous fixes, removed 'Option 1' as pandas code runs at the same speed|

# # Setup

# ## Imports

# In[ ]:


import io
import os
import firecloud.api as fapi
import google.cloud.storage

# Only used in multiple children use case
import pandas as pd
import logging


# ## User-set variables

# In[ ]:


# Don't forgot the quotation marks!
SUBDIRECTORY="/your_directory_here/" # Make sure to include slashes
TABLE_NAME="table_name" #Do not include spaces or underscores
PARENT_FILETYPE="cram"


# ## Environmental variables

# In[ ]:


# You don't need to change these
BILLING_PROJECT_ID = os.environ['GOOGLE_PROJECT']
WORKSPACE = os.path.basename(os.path.dirname(os.getcwd()))
BUCKET = os.environ["WORKSPACE_BUCKET"]


# ## Call FireCloud and set directory

# In[ ]:


try:
    response = fapi.list_entity_types(BILLING_PROJECT_ID, WORKSPACE)
    if response.status_code != 200:
        print("Error in Firecloud, check your billing project ID and the name of your workspace.")
        raise
    else:
        print("Firecloud has found your workspace!")
        directory = BUCKET + SUBDIRECTORY
except NameError:
    print("Caught a NameError exception. This may mean the kernal was restarted or you didn't run ",
          "the cells above. Try running the cells above again.")
    raise


# ## Display the contents of your workspace bucket (optional)
# You may want to avoid running the cell below if you have a large number of files in your bucket.

# In[ ]:


get_ipython().system('gsutil ls $directory')


# If you get a CommandError exception here, it may be because your SUBDIRECTORY is for a psuedofolder that doesn't actually exist. Try `!gsutil ls $bucket` and make sure the directory you're looking for actally exists. If it's not there, run the Psuedofolder Maker notebook first.

# ## Delete placeholder file (if you used Psuedofolder Maker)
# Since there's now other files in the psuedofolder, you can delete the placeholder file that Psuedofolder Maker made in order to prevent it from showing up in your TSV.

# In[ ]:


get_ipython().system('gsutil rm gs${directory}placeholder')


# # Do magic to create a TSV file

# The following code will create a TSV file, which follows a format used by Terra's data tables.
# ### Expected runtime
# | parent files | child 1 files | child 2 files | child 3 files | estimated time |  
# | --- | --- | --- | --- | --- |
# | 1000 | 1000 | 0 | 0 | 30 seconds |  
# | 1000 | 1000 | 1000 | 1000 | 75 seconds |  

# ## Setup

# In[ ]:


logger = logging.getLogger('')
logger.setLevel(logging.INFO)
storage_client = google.cloud.storage.Client()
google_storage_prefix = 'gs://'
if BUCKET.startswith(google_storage_prefix):
    bucket = BUCKET[len(google_storage_prefix):]  # clip off "gs://" prefix
subdirectory_chopped = SUBDIRECTORY.strip("/")


# ## Check number of "children" to link to parent
# Unlike File Finder, this parses the output of `gsutil ls` directly. **As a result, if your filenames contain non-ascii (ie, stuff besides A-Z, a-z, underscores, and dashes) or bizarre characters (ie, newlines) there is a chance this will not work as expected.**

# In[ ]:


# Create dataframe
df = pd.DataFrame()

# Parse output
logging.info("Querying your GCS bucket for extensions...")
get_ipython().system('gsutil ls $directory > ls.txt')
with open("ls.txt", "r") as this_file:
    df['location'] = this_file.read().splitlines()

# Split on file extension and count how many child file extensions there are
df['FileType'] = df.location.str.rsplit('.', 1).str[-1]
unique_children = pd.unique(df[['FileType']].values.ravel("K"))
logging.info("{0} unique file extensions (including parent) have been found.".format(len(unique_children)))
unique_children = unique_children[unique_children!=PARENT_FILETYPE]
logging.info("Child file types are: %s  If this looks wrong, check your GCS directory for missing or junk files." % unique_children)


# ## Link files together

# In[ ]:


def progress(current_child, unique_children):
    if current_child not in unique_children:
        # Should never happen unless user edits code incorrectly
        logging.error("Invalid child extension")
        raise
    else:
        unique_children_list = unique_children.tolist()
        logging.info("\t\t Processing {0} files... ({1} out of {2})".format(current_child, 
                      unique_children_list.index(current_child)+1, 
                      len(unique_children_list))

# Link parents and children; this part takes the longest
logging.info("Linking parents and children...")
list_dfs = [] # List of dataframes, one df per child extension
for child in unique_children:
    list_of_list_child = []
    progress(child, unique_children)    
    for blob in storage_client.list_blobs(bucket, prefix=subdirectory_chopped):
        if blob.name.endswith(PARENT_FILETYPE):
            # remove PARENT_FILETYPE extension and search for basename
            basename = blob.name[:-len(f'.{PARENT_FILETYPE}')]
            for basename_blob in storage_client.list_blobs(bucket, prefix=basename):
                # only add a line if there is a corresponding child file
                if basename_blob.name.endswith(child):
                    parent_filename = blob.name.split('/')[-1]
                    child_filename = basename_blob.name.split('/')[-1]
                    parent_location = f'{google_storage_prefix}{bucket}/{blob.name}'
                    child_location  = f'{google_storage_prefix}{bucket}/{basename_blob.name}'
                    list_child = ([parent_filename, parent_location, child_filename, child_location])
                    list_of_list_child.append(list_child)
    df_child = pd.DataFrame(list_of_list_child, columns=['parentFile', 'parentLocation',child+'File', child+'Location'])
    list_dfs.append(df_child)


# ## Merge dataframes on shared "parent" file

# In[ ]:


logging.info("Merging dataframes...")
merged_df = pd.DataFrame()
for df in list_dfs:
    try:
        merged_df = merged_df.merge(df, on=['parentFile', 'parentLocation'])
    except KeyError:
        # Should only happen first iteration
        merged_df = df
logging.info("Finished!")


# ## Inspect dataframe (optional)

# Panadas will not display all of your rows if there are many of them.

# In[ ]:


print(merged_df)


# ## Generate TSV file from dataframe and upload it

# In[ ]:


merged_df.to_csv("dataframe.tsv", sep='\t')

# Format resulting TSV file to play nicely with Terra 
with open('dataframe.tsv', "r+") as file1:
    header = file1.readline()
    everything_else = file1.readlines()
full_header="entity:"+TABLE_NAME+"_id"+header
with open('final.tsv', "a") as file2:
    file2.write(full_header)
    for string in everything_else:
        # Zfill the index
        columns = string.split('\t')
        columns[0] = columns[0].zfill(5)
        file2.write('\t'.join(columns))
    
# Clean up
response = fapi.upload_entities_tsv(BILLING_PROJECT_ID, WORKSPACE, "final.tsv", "flexible")
fapi._check_response_code(response, 200)
get_ipython().system('rm dataframe.tsv')
get_ipython().system('rm final.tsv')


# Now, in your data section, you will see a new table with the name you set as the value of `TABLE_NAME`.
