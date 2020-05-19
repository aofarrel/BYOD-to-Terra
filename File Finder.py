#!/usr/bin/env python
# coding: utf-8

# # BYOD -- File Finder
#  
# ## Goal
# Generate a Terra data table from files that a user has uploaded into a workspace bucket.
# 
# This notebook is named File Finder in the sense that the location of your BYOD files will be transferred to a Terra data table that includes their Google Cloud Storage addresses, allowing them to easily be found by workflows. To be clear, the contents of the files themselves are not parsed by this notebook, just their file name and their address.
# 
# This notebook is a more user-friendly Jupyter version of [tsvify.sh](https://github.com/DataBiosphere/BYOD-to-Terra/blob/anvil/tsvify.sh) that runs in the cloud instead of on a local machine.
# 
# ## Assumptions
# 1) You have a psuedofolder in your Terra data section that contains BYOD data in the form of files -- see [documentation here](https://github.com/DataBiosphere/BYOD-to-Terra/blob/anvil/full_documentation.md) if you need help uploading your files 
# 2) All of the files you want in your data table are in the same psuedofolder, and are not in sub-psuedofolders  
# 3) You've uploaded all your files already -- Terra data tables are not dynamic, so if you want to add more later, you'll have to re-run this notebook  
# 4) You are not trying to overwrite a data table you have already created. If you re-run this notebook and set TABLE_NAME to something that already exists as a Terra data table, the old table will NOT to overwritten. You will need to either pick a new name or delete the old table first.

# Version history:
# 
# | v | date | author | notes |
# | --- | --- | --- | --- |
# | 0.9 | April 6th 2020 | Ash | initial |
# | 1.0 | April 8th 2020 | Ash | now calls environmental variables, cleaned up, implented notes from PR 17 of terra-data-utils  |
# | 1.1 | April 13th 2020 | Ash | better intro, placeholder cleanup, pythonic parse of GCS (thanks Lon!)  |
# | 1.2 | April 27th 2020 | Ash |   fixed NameError exception notice, rearranged for clarity and consistency across notebooks|

# # Setup

# ## Imports

# In[ ]:


import io
import os
import firecloud.api as fapi
import google.cloud.storage


# ## User-set variables

# In[ ]:


# Don't forgot the quotation marks!
SUBPREFIX="/test_files/"
TABLE_NAME="byod"


# In rare circumstances you may need to escape the slashes in your SUBPREFIX variable. If you run into errors, follow this style of formating: \\/test_files\\/

# ## Environment variables

# In[ ]:


BILLING_PROJECT_ID = os.environ['GOOGLE_PROJECT']
WORKSPACE = os.path.basename(os.path.dirname(os.getcwd()))
BUCKET = os.environ["WORKSPACE_BUCKET"]


# ## Call Firecloud

# In[ ]:


try:
    response = fapi.list_entity_types(BILLING_PROJECT_ID, WORKSPACE)
    if response.status_code != 200:
        print("Error in Firecloud, check your billing project ID and the name of your workspace.")
        raise
    else:
        print("Firecloud has found your workspace!")
        directory = BUCKET + SUBPREFIX
except NameError:
    print("Caught a NameError exception. This may mean the kernal was restarted or you didn't run ",
          "the cells above. Try running the cells above again.")
    raise
    
storage_client = google.cloud.storage.Client()


# ## Display the contents of your workspace bucket
# This step is *optional* and you might want to skip it if you have a lot of files in your bucket.

# In[ ]:


get_ipython().system('gsutil ls $directory')


# If you get a CommandError exception here, it may be because your SUBPREFIX is for a psuedo-folder that doesn't actually exist. Try `!gsutil ls $bucket` and make sure the directory you're looking for actally exists. If it's not there, run the Folder-Maker notebook first.

# ## Delete placeholder file (if you created folder with Folder Maker)
# Since there's now other files in the psuedo-folder (see Folder Maker for more info on why we are calling this a psuedo-folder), you can delete the placeholder file that Folder Maker made in order to prevent it from showing up in your TSV. If you get a "no URLS matched" error, the placeholder file has likely already been deleted.

# In[ ]:


get_ipython().system('gsutil rm $BUCKET$SUBPREFIX"placeholder"')


# ## Do magic to create a TSV file

# In[ ]:


bucket_no_gs = BUCKET[5:]
psuedofolder = SUBPREFIX[1:-1]
with open('final.tsv', 'w') as f:
    f.write(f"entity:{TABLE_NAME}_id\tfile_location\n")
    for file_path in storage_client.list_blobs(bucket_no_gs, prefix=psuedofolder):
        s = file_path.name
        f.write(f'{s.split("/")[-1]}\t{BUCKET}/{s}\n')


# Again, if you get a CommandError exception, there is probably a problem with your SUBPREFIX. Make sure to fix that, or else your resulting TSV will be blank.

# ## Inspect TSV file (optional, you may want to skip this if you're dealing with lots of files)

# In[ ]:


get_ipython().system('cat final.tsv')


# ## Upload TSV file as a Terra data table

# In[ ]:


response = fapi.upload_entities_tsv(BILLING_PROJECT_ID, WORKSPACE, "final.tsv", "flexible")
fapi._check_response_code(response, 200)

