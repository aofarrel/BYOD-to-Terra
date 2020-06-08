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

# ## Version history:
# 
# | v | date | author | notes |
# | --- | --- | --- | --- |
# | 0.9 | April 6th 2020 | Ash | initial |
# | 1.0 | April 13th 2020 | Ash, Lon |   fixed multiple children and no parent usecases, implemented Lon's no-pandas code + Brian and Lon's other suggestions, better intro|
# | 1.1 | April 14th 2020 | Ash |   made Pandas code significantly faster (n=3000 files split over 1 parent and 2 children: 340 s --> 165 s)|
# | 1.2 | April 27th 2020 | Ash |   fixed NameError exception notice, rearranged for clarity and consistency across notebooks|
# | 1.3 | May 19th 2020 | Ash |   better logging, indeces now zfilled, parents now sort at front in panadas code, minor clarifications|
# | 1.4 | June 8th 2020 | Ash |   vastly improved pandas code, miscellaneous fixes|

# # Setup

# ## Imports

# In[1]:


import io
import os
import firecloud.api as fapi
import google.cloud.storage

# Only used in multiple children use case
import pandas as pd
import logging


# ## User-set variables

# In[74]:


# Don't forgot the quotation marks!

SUBDIRECTORY="/thousands/" # Make sure to include slashes
TABLE_NAME="final" #Do not include spaces or underscores
PARENT_FILETYPE="cram"

# If your filenames are like this, please set INCLUDE_PARENT_EXTENSION to True:
# NWD119844.CRAM
# NWD119844.CRAM.CRAI

# If you filenames are like this, please set INCLUDE_PARENT_EXTENSION to False:
# NWD119844.CRAM
# NWD119844.CRAI

# No quotation marks for this variable, just True or False
INCLUDE_PARENT_EXTENSION = True


# ## Environmental variables

# In[53]:


BILLING_PROJECT_ID = os.environ['GOOGLE_PROJECT']
WORKSPACE = os.path.basename(os.path.dirname(os.getcwd()))
BUCKET = os.environ["WORKSPACE_BUCKET"]


# ## Call FireCloud

# In[54]:


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


# ## Display the contents of your workspace bucket
# This step is *optional* and you might want to skip it if you have a lot of files in your bucket.

# In[9]:


get_ipython().system('gsutil ls $directory')


# If you get a CommandError exception here, it may be because your SUBPREFIX is for a psuedo-folder that doesn't actually exist. Try `!gsutil ls $bucket` and make sure the directory you're looking for actally exists. If it's not there, run the Folder-Maker notebook first.

# ## Delete placeholder file (if you used Psuedofolder Maker)
# Since there's now other files in the psuedofolder, you can delete the placeholder file that Folder Maker made in order to prevent it from showing up in your TSV.

# In[ ]:


get_ipython().system('gsutil rm gs${directory}placeholder')


# # Do magic to create a TSV file

# Two methods have been included here.
# 
# **Option 1, Single Child**, does NOT support the following use cases:
# * There is more than one child per parent
# * INCLUDE_PARENT_EXTENSION = False
# * You don't want to specify the child file extension  
# 
# **Option 2** supports all of these use cases, plus the single child use case. However, it might run slower than Option 1 if you have thousands of files.
# 
# ### Expected runtimes
# | parent files | child 1 files | child 2 files | child 3 files | estimated time for Option 1 | estimated time for Option 2 |  
# | --- | --- | --- | --- | --- | --- |  
# | 1000 | 1000 | 0 | 0 | 30 seconds | 30 seconds |  
# | 1000 | 1000 | 1000 | 1000 | cannot handle | 75 seconds |  

# ## Option 1: Single Child
# This will ONLY assign a single child to each parent. Additional children will be ignored. You must write the CHILD_FILETYPE in the cell below.

# In[38]:


CHILD_FILETYPE = "crai"


# In[55]:


# Based on code written by Lon Blauvelt (UCSC)

storage_client = google.cloud.storage.Client()
google_storage_prefix = 'gs://'

if BUCKET.startswith(google_storage_prefix):
    bucket = BUCKET[len(google_storage_prefix):]  # clip off "gs://" prefix
subdirectory_chopped = SUBDIRECTORY.strip("/")
i = 0
with open(TABLE_NAME, 'w') as f:
    header = '\t'.join([f'entity:{TABLE_NAME}_id', 'filename', 'location',
                        'parent_file_ext', CHILD_FILETYPE, f'{CHILD_FILETYPE}_location'])
    f.write(header + '\n')
    for blob in storage_client.list_blobs(bucket, prefix=subdirectory_chopped):
        if blob.name.endswith(PARENT_FILETYPE):
            # remove PARENT_FILETYPE extension and search for basename
            basename = blob.name[:-len(f'.{PARENT_FILETYPE}')]
            for basename_blob in storage_client.list_blobs(bucket, prefix=basename):
                # only add a line if there is a corresponding child file
                if basename_blob.name.endswith(CHILD_FILETYPE):
                    table_id = f'{i}'
                    table_id = table_id.zfill(4)
                    parent_filename = blob.name.split('/')[-1]
                    child_filename = basename_blob.name.split('/')[-1]
                    parent_location = f'{google_storage_prefix}{bucket}/{blob.name}'
                    child_location  = f'{google_storage_prefix}{bucket}/{basename_blob.name}'
                    line = '\t'.join([table_id, parent_filename, parent_location,
                                     PARENT_FILETYPE, child_filename, child_location])
                    f.write(line + '\n')
                    i += 1


# ### Check output
# This is optional and you may want to skip it if you have a lot of files.

# In[33]:


with open(TABLE_NAME, 'r') as f:
    print(f.read())


# ### Upload TSV to Terra

# In[48]:


response = fapi.upload_entities_tsv(BILLING_PROJECT_ID, WORKSPACE, TABLE_NAME, "flexible")
fapi._check_response_code(response, 200)
get_ipython().system('rm $TABLE_NAME')


# ## Option 2: Multiple children

# Unlike File Finder or Option 1, this parses the output of `gsutil ls` directly. **As a result, if your filenames contain non-ascii (ie, stuff besides A-Z, a-z, underscores, and dashes) or bizarre characters (ie, newlines) there is a chance this will not work as expected.**

# In[61]:


logger = logging.getLogger('')
logger.setLevel(logging.INFO)

storage_client = google.cloud.storage.Client()
google_storage_prefix = 'gs://'
if BUCKET.startswith(google_storage_prefix):
    bucket = BUCKET[len(google_storage_prefix):]  # clip off "gs://" prefix
subdirectory_chopped = SUBDIRECTORY.strip("/")

def progress(current_child, unique_children):
    if current_child not in unique_children:
        # Should never happen unless user edits code incorrectly
        logging.error("Invalid child extension")
        raise
    else:
        unique_children_list = unique_children.tolist()
        logging.info("\t\t Processing {0} files... ({1} out of {2})".format(current_child, 
                      unique_children_list.index(current_child)+1, 
                      len(unique_children_list)))

# Create dataframe
df = pd.DataFrame()

##Might be possible to do this in just one step. Skip anything that matches the parent file name.
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

# Link parents and children; this part takes the longest
logging.info("Linking parents and children...")
list_dfs = [] # List of dataframes, one df per child extension
for child in unique_children:
    i = 0
    list_of_list_child = []
    progress(child, unique_children)    
    for blob in storage_client.list_blobs(bucket, prefix=subdirectory_chopped):
        if blob.name.endswith(PARENT_FILETYPE):
            # remove PARENT_FILETYPE extension and search for basename
            basename = blob.name[:-len(f'.{PARENT_FILETYPE}')]
            for basename_blob in storage_client.list_blobs(bucket, prefix=basename):
                # only add a line if there is a corresponding child file
                if basename_blob.name.endswith(child):
                    i += 1
                    table_id = f'{i}'.zfill(4)
                    parent_filename = blob.name.split('/')[-1]
                    child_filename = basename_blob.name.split('/')[-1]
                    parent_location = f'{google_storage_prefix}{bucket}/{blob.name}'
                    child_location  = f'{google_storage_prefix}{bucket}/{basename_blob.name}'
                    list_child = ([table_id, parent_filename, parent_location, child_filename, child_location])
                    list_of_list_child.append(list_child)
    df_child = pd.DataFrame(list_of_list_child, columns=['ID','parentFile', 'parentLocation',child+'File', child+'Location'])
    list_dfs.append(df_child)


# In[75]:


#Merge child dataframes on shared file name
logging.info("Merging dataframes...")
merged_df = pd.DataFrame()
for df in list_dfs:
    try:
        merged_df = merged_df.merge(df, on=['parentFile', 'parentLocation', 'ID'])
    except KeyError:
        # Should only happen first iteration
        merged_df = df
logging.info("Finished!")


# ### Inspect dataframe

# This is another optional step. Panadas will not display all of your rows if there are many of them.

# In[76]:


print(merged_df)


# ### Generate TSV file from dataframe and upload it

# In[78]:


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


# In[ ]:




