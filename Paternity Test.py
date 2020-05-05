#!/usr/bin/env python
# coding: utf-8

# # BYOD -- Paternity Test
#  
# ## Goal
# Generate a Terra data table from files that a user has uploaded into a workspace bucket, where each row represents a "parent" file and has cells for its "children." Parent files are things like CRAM, and child files are things like CRAI. It is named Paternity Test in the sense that parents and children are linked in the same table. What is determined to be a parent is set by the user specifying a file extension. Only one parent is allowed per data table.
# 
# This sort of thing can be done on a local UNIX/UNIX-like machine using shell scripts, but that isn't ideal for certain BYOD scenarios. We need a way to do it programmatically (as there might be hundreds of files) and on the Terra platform itself (as the files might be coming from Windows, or the user doesn't know how to run a shell script), hence the motivation for this notebook's creation.
# 
# ## Assumptions
# 1) You have a psuedo-folder in your Terra data section that contains BYOD data in the form of files  
# 2) All of the files you want in your data table are in the same psuedo-folder, and are not in sub-psuedo-folders  
# 3) You've uploaded all your files already -- Terra data tables are not dynamic, so if you want to add more later, you'll have to re-run this notebook  
# 4) You are not trying to overwrite a data table you have already created. If you re-run this notebook and set TABLE_NAME to something that already exists as a Terra data table, the old table will NOT to overwritten. You will need to either pick a new name or delete the old table first.  
# 
# You do NOT have to have run File Finder before this notebook. The difference between this and File Finder is that File Finder will create a row for every file in your psuedo-folder, while Paternity Test will create a row for every parent file.
# 
# Files that lack the parent file extension and are not children will NOT be added to the data table.

# Version history:
# 
# | v | date | author | notes |
# | --- | --- | --- | --- |
# | 0.9 | April 6th 2020 | Ash | initial |
# | 1.0 | April 13th 2020 | Ash, Lon |   fixed multiple children and no parent usecases, implented Lon's no-pandas code, implemented Brian and Lon's other suggestions, better intro|
# | 1.1 | April 14th 2020 | Ash |   made Pandas code significantly faster|
# | 1.2 | April 27th 2020 | Ash |   fixed NameError exception notice, rearranged for clarity and consistency across notebooks|
# 

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
SUBDIRECTORY="/thousands/"
TABLE_NAME="paternity_test_test" #Do not include spaces or weird characters
PARENT_FILETYPE="cram"

# If your filenames are like this, please set INCLUDE_PARENT_EXTENSION to True:
# NWD119844.CRAM
# NWD119844.CRAM.CRAI

# If you filenames are like this, please set INCLUDE_PARENT_EXTENSION to False:
# NWD119844.CRAM
# NWD119844.CRAI

# No quotation marks for this variable, just True or False
INCLUDE_PARENT_EXTENSION = True


# Make sure to include slashes in your SUBDIRECTORY variable.

# ## Environmental variables

# In[ ]:


BILLING_PROJECT_ID = os.environ['GOOGLE_PROJECT']
WORKSPACE = os.path.basename(os.path.dirname(os.getcwd()))
BUCKET = os.environ["WORKSPACE_BUCKET"]


# ## Call FireCloud

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


# ## Display the contents of your workspace bucket
# This step is *optional* and you might want to skip it if you have a lot of files in your bucket.

# In[ ]:


get_ipython().system('gsutil ls $directory')


# If you get a CommandError exception here, it may be because your SUBPREFIX is for a psuedo-folder that doesn't actually exist. Try `!gsutil ls $bucket` and make sure the directory you're looking for actally exists. If it's not there, run the Folder-Maker notebook first.

# ## Delete placeholder file (if you created folder with Folder Maker)
# Since there's now other files in the psuedo-folder (see Folder Maker for more info on why we are calling this a psuedo-folder), you can delete the placeholder file that Folder Maker made in order to prevent it from showing up in your TSV.

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
# | parent files | child 1 files | child 2 files | estimated time for Option 1 | estimated time for Option 2 |  
# | --- | --- | --- | --- | --- |  
# | 1000 | 1000 | 0 | 35 seconds | 130 seconds |  
# | 1000 | 1000 | 1000 | cannot handle | 340 seconds |  

# ## Option 1: Single Child
# This will ONLY assign a single child to each parent. Additional children will be ignored. You must write the CHILD_FILETYPE in the cell below.

# In[ ]:


CHILD_FILETYPE = "crai"


# In[ ]:


# Based on code written by Lon Blauvelt (UCSC)

storage_client = google.cloud.storage.Client()
google_storage_prefix = 'gs://'

if BUCKET.startswith(google_storage_prefix):
    bucket = BUCKET[len(google_storage_prefix):]  # clip off "gs://" prefix
if SUBDIRECTORY.startswith("/"):
    subdirectory_chopped = SUBDIRECTORY[1:len(SUBDIRECTORY)]  # clip off starting /
if subdirectory_chopped.endswith("/"):
    subdirectory_chopped = subdirectory_chopped[:len(subdirectory_chopped)-1]  # clip off trailing /
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

# In[ ]:


with open(TABLE_NAME, 'r') as f:
    print(f.read())


# ### Upload TSV to Terra

# In[ ]:


response = fapi.upload_entities_tsv(BILLING_PROJECT_ID, WORKSPACE, TABLE_NAME, "flexible")
fapi._check_response_code(response, 200)


# ## Option 2: Multiple children

# Unlike File Finder or Option 1, this parses the output of `gsutil ls` directly. **As a result, if your filenames contain non-ascii (ie, stuff besides A-Z, a-z, underscores, and dashes) or bizarre characters (ie, newlines) there is a chance this will not work as expected.**

# In[ ]:


logger = logging.getLogger('')
logger.setLevel(logging.INFO)
def baseID(filename_string, child_extension):
    global PARENT_FILETYPE
    global INCLUDE_PARENT_EXTENSION
    if INCLUDE_PARENT_EXTENSION:
        fileID = filename_string.replace("."+child_extension,"")
    else:
        fileID = filename_string.replace("."+PARENT_FILETYPE+"."+child_extension,"")
    return fileID

# Get location of everything and their file names
logging.info("Querying Google...")
get_ipython().system('gsutil ls $directory > contentlocations.txt')
logging.info("Processing filenames...")
get_ipython().system("cat contentlocations.txt | sed 's@.*/@@' > filenames.txt")

# Import everything
logging.info("Constructing dataframe...")
data={}
this_file=open("contentlocations.txt", "r")
lineslocation = this_file.read().splitlines()
this_file.close()
that_file=open("filenames.txt", "r")
linesfilename = that_file.read().splitlines()
data['filename'] = linesfilename
data['location'] = lineslocation #here in order to put columns in a particular order without reassigning later

# Create dataframe
df = pd.DataFrame(data)

# Split on file extension
df['FileType'] = df.filename.str.rsplit('.', 1).str[-1]

# Count file extensions to see how many child file types there are
unique_children = pd.unique(df[['FileType']].values.ravel("K"))
unique_children = unique_children[unique_children!=PARENT_FILETYPE]

# Play with strings to get basic ID, needed to link parents and children
logging.info("Manipulating dataframe...")
i = 0
for child_extension in unique_children: #loop once per child extension
    i=i+1
    logging.debug("\t* Dealing with %s (child %d out of %d)" % (child_extension, i, len(unique_children)))
    df[child_extension] = ""
    df[child_extension+"_location"] = ""
    for index_label, row_series in df.iterrows():
        # On first iteration, iterate over parent files
        # Only do once to avoid wasting time repeating this constantly
        if (df.at[index_label,'FileType'] == PARENT_FILETYPE) & (i==1):
            parent_baseID = baseID(row_series['filename'], child_extension)
            df.at[index_label,'ID'] = parent_baseID
        # Only iterate over children that match the child_extension
        # This avoids overwriting if there's more than one child_extension
        elif df.at[index_label,'FileType'] == child_extension:
            child_baseID = baseID(row_series['filename'], child_extension)
            df.at[index_label,'ID'] = child_baseID
            #output_list.update({baseID(row_series['filename'], child_extension) : df.at[index_label,'FileType']})
        else:
            pass
# For thousands of files this takes a long time, let's try to show some sort of progress.
# Progress bar packages that use carriage returns don't play nicely with Jupyter when cells
# are ran more than once, so a balance between the user being updated and not making several
# lines of text is needed.
if(df.shape[0] > 100):
    triggerevery = df.shape[0] // 5
    nexttrigger = triggerevery
else:
    triggerevery = None

logging.info("Child file types are: %s  If this looks wrong, check your GCS directory for missing or junk files." % unique_children)
    
# Iterate yet again to match children with their parents
logging.info("Matching children and parents...")
i = 0 #only used for logging
j = 0
for child_extension in unique_children: #loop once per child extension
    i=i+1
    logging.info("\t* Looking at %s files (child %d out of %d)" % (child_extension, i, len(unique_children)))
    
    # Iterate df, where each row represents one file of any type
    for index_label, row_series in df.iterrows():

        # If user has a big dataframe, this will give a simple progress update
        if triggerevery is not None:
            j=j+1
            if(j==nexttrigger):
                logger.info("\t\t* %d / %d" % (nexttrigger, df.shape[0]))
                nexttrigger = nexttrigger + triggerevery
                if nexttrigger >= df.shape[0]:
                    nexttrigger = triggerevery

        # If, at this row, we have found a file with the parent extension
        try:
            if(df.at[index_label,'FileType'] == PARENT_FILETYPE):

                # Find this parent's child
                # Child might be above parent so we can't just start from index of the parent
                for index_label_inception, row_series_inception in df.iterrows():
                    logging.debug("Outer iter %d, inner iter %d parent %s checking if %s is its child" % (index_label, index_label_inception, df.at[index_label,'filename'], df.at[index_label_inception,'filename']))
                    if index_label != index_label_inception: #needed to preventing it find itself
                        if(df.at[index_label,'ID'] == #if parent ID...
                           df.at[index_label_inception,'ID']) and ( #equals potential child ID, and...
                            df.at[index_label_inception,'FileType'] == #child's file extension...
                            child_extension): #equals the current child_extension we are iterating on
                            # Child found!
                            logging.debug("    Found "+df.at[index_label_inception,'filename']+" to be child")
                            df.at[index_label,child_extension] = df.at[index_label_inception,'filename']
                            df.at[index_label,child_extension+"_location"] = df.at[index_label_inception,'location']
                            df.drop([index_label_inception], inplace=True)
                            index_label = index_label+1 # Adjust index accordingly
                            break

                # We were unable to find a child for this parent
                if(df.at[index_label,child_extension] == ""):
                    logging.warning("Could not find child of type %s for parent %s" % (child_extension, df.at[index_label, 'FileType']))
                    df.at[index_label,child_extension] = ""
                    df.at[index_label,child_extension+"_location"] = ""
        except KeyError:
            pass
logging.info("Cleaning up dataframe...")           
# Iterate one more time to delete child rows
# Because children could appear above their parents, deleting during the above iteration could mess things up
df.drop(columns=['ID'], inplace=True)
df.rename(columns = {'FileType':'parent_file_ext'}, inplace = True)
for index_label, row_series in df.iterrows():
        if(df.at[index_label,'parent_file_ext'] != PARENT_FILETYPE):
            df.drop([index_label], inplace=True)
df.reset_index(inplace=True, drop=True)
logging.info("Finished")


# ### Inspect dataframe

# This is another optional step. Panadas will not display all of your rows if there are many of them.

# In[ ]:


print(df)


# ### Generate TSV file from dataframe and upload it

# In[ ]:


#!rm final.tsv
# ^ Uncomment above line if you will be running this block more than once

df.to_csv("dataframe.tsv", sep='\t')

# Format resulting TSV file to play nicely with Terra 
with open('dataframe.tsv', "r+") as file1:
    header = file1.readline()
    everything_else = file1.readlines()
    file1.close()
full_header="entity:"+TABLE_NAME+"_id"+header
with open('final.tsv', "a") as file2:
    file2.write(full_header)
    for string in everything_else:
        file2.write(string)
    file2.close()

# Clean up
get_ipython().system('rm dataframe.tsv')
response = fapi.upload_entities_tsv(BILLING_PROJECT_ID, WORKSPACE, "final.tsv", "flexible")
fapi._check_response_code(response, 200)

