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

# In[6]:


# Don't forgot the quotation marks!

SUBDIRECTORY="/lessEdgeCases/" # Make sure to include slashes
TABLE_NAME="table_name" #Do not include spaces or underscores
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

# In[3]:


BILLING_PROJECT_ID = os.environ['GOOGLE_PROJECT']
WORKSPACE = os.path.basename(os.path.dirname(os.getcwd()))
BUCKET = os.environ["WORKSPACE_BUCKET"]


# ## Call FireCloud

# In[8]:


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
# | parent files | child 1 files | child 2 files | estimated time for Option 1 | estimated time for Option 2 |  
# | --- | --- | --- | --- | --- |  
# | 1000 | 1000 | 0 | 35 seconds | 130 seconds |  
# | 1000 | 1000 | 1000 | cannot handle | 165 seconds |  

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

# In[ ]:


with open(TABLE_NAME, 'r') as f:
    print(f.read())


# ### Upload TSV to Terra

# In[ ]:


response = fapi.upload_entities_tsv(BILLING_PROJECT_ID, WORKSPACE, TABLE_NAME, "flexible")
fapi._check_response_code(response, 200)
get_ipython().system('rm $TABLE_NAME')


# ## Option 2: Multiple children

# Unlike File Finder or Option 1, this parses the output of `gsutil ls` directly. **As a result, if your filenames contain non-ascii (ie, stuff besides A-Z, a-z, underscores, and dashes) or bizarre characters (ie, newlines) there is a chance this will not work as expected.**

# In[5]:


1) query with gsutil just to get a list of file extensions and NOTHING else
2) do lon's code except instead of matching one child extension, match one from a list


WAIT
if something shares the basename of a parent, and it is not the parent itself, then it must be a child. you don't need to know what the child filetype is.
...errr maybe you do to make sure your columns each only habve one file type

current code takes 7 minutes


# In[10]:


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


# Option 1: Make one dataframe per child and then merge them on the parent at the end
# Option 2: Make a dataframe linking parents and children
# Option 3: Don't use dataframes
# Option 2 would require iterating the parent dataframe constantly while option 1 should just do that once

# It is faster to keep a list and construct a df at the end of each child then keep adding to a df

logging.info("Linking parents and children...")
list_dfs = [] # List of dataframes, one df per child extension
for child in unique_children:
    list_of_list_child = []
    progress(child, unique_children)    
    i = 0
    for blob in storage_client.list_blobs(bucket, prefix=subdirectory_chopped):
        if blob.name.endswith(PARENT_FILETYPE):
            # remove PARENT_FILETYPE extension and search for basename
            basename = blob.name[:-len(f'.{PARENT_FILETYPE}')]
            for basename_blob in storage_client.list_blobs(bucket, prefix=basename):
                # only add a line if there is a corresponding child file
                if basename_blob.name.endswith(child):
                    table_id = f'{i}'.zfill(4)
                    #table_id = table_id.zfill(4)
                    parent_filename = blob.name.split('/')[-1]
                    child_filename = basename_blob.name.split('/')[-1]
                    parent_location = f'{google_storage_prefix}{bucket}/{blob.name}'
                    child_location  = f'{google_storage_prefix}{bucket}/{basename_blob.name}'
                    list_child = ([table_id, parent_filename, parent_location, child_filename, child_location])
                    i += 1
                    list_of_list_child.append(list_child)
    df_child = pd.DataFrame(list_of_list_child, columns=['ID','parentFile', 'parentLocation',child+'File', child+'Location'])
    list_dfs.append(df_child)


# In[23]:


#Merge child dataframes on shared file name
logging.info("Merging dataframes...")
merged_df = pd.DataFrame()
for df in list_dfs:
    try:
        merged_df = merged_df.merge(df, on=['parentFile', 'parentLocation', 'ID'])
    except KeyError:
        merged_df = df
logging.info("Finished!")
logging.info(merged_df)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[14]:


def baseID(filename_string, child_extension):
    if INCLUDE_PARENT_EXTENSION:
        fileID = filename_string.replace("."+child_extension,"")
    else:
        fileID = filename_string.replace("."+PARENT_FILETYPE+"."+child_extension,"")
    return fileID

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
        else:
            pass

logging.info("Child file types are: %s  If this looks wrong, check your GCS directory for missing or junk files." % unique_children)
    
# Iterate yet again to match children with their parents
logging.info("Matching children and parents...")
current_child = 0 #only used for logging

for child_extension in unique_children: #loop once per child extension
    
    current_child=current_child+1
    logging.info("\t* Looking at %s files (child %d out of %d)" % (child_extension, current_child, len(unique_children)))
    
    # For thousands of files this takes a long time, let's try to show some sort of progress.
    # Progress bar packages that use carriage returns don't play nicely with Jupyter when cells
    # are ran more than once, so a balance between the user being updated and not making several
    # lines of text is needed.
    j = 0
    if(df.shape[0] > 100):
        triggerevery = df.shape[0] // 5
        nexttrigger = triggerevery
    else:
        triggerevery = None
    
    # Iterate df, where each row represents one file of any type
    for index_label, row_series in df.iterrows():

        # If user has a big dataframe, this will give a simple progress update
        if triggerevery is not None:
            j=j+1
            if(j==nexttrigger):
                logger.info("\t\t* %d rows processed, %d parent or unmatched child rows remaining in dataframe" % (nexttrigger, df.shape[0]))
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
for index_label, row_series in df.iterrows():
        if(df.at[index_label,'FileType'] != PARENT_FILETYPE):
            df.drop([index_label], inplace=True)
df.drop(columns=['FileType'], inplace=True)
df.rename(columns = {'filename':'-parent_file', 'location':'-parent_location'}, inplace = True)
df.reset_index(inplace=True, drop=True)
logging.info("Finished")


# ### Inspect dataframe

# This is another optional step. Panadas will not display all of your rows if there are many of them.

# In[ ]:


print(df)


# ### Generate TSV file from dataframe and upload it

# In[ ]:


df.to_csv("dataframe.tsv", sep='\t')

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

