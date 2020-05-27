#!/usr/bin/env python
# coding: utf-8

# # Introduction
# Google Cloud Storage is a great tool, but it has an odd way of handling folders. That is to say, it doesn't. There is no equivalent of `mkdir` in Google Cloud Storage. Luckily, with a few tricks, we can get around this limitation and create a psuedofolder in your workspace's data section, helping keep your BYOD files organized.
# 
# Please be sure to read the use case below -- **this notebook is *not* needed in most BYOD cases.** It is being provided only for the few use cases it does benefit, and to shed light on how Google Cloud Storage's file system works.

# Version history:
# 
# | v | date | author | notes |
# | --- | --- | --- | --- |
# | 0.9 | April 3rd 2020 | Ash | initial |
# | 1.0 | April 8th 2020 | Ash | now calls environmental variables, fixed notes on kernal restarting, more explanation
# | 1.1 | May 18th 2020 | Ash | clarified use case, changed name from Folder Maker to more accurate Psuedofolder Maker

# ## Use case
# If you will be using gsutil to move your files into your workspace bucket, *you do not need this notebook.* Instead, simply add the desired folder name to your gsutil cp command, such as `gsutil cp gs://source/file.cram gs://destination/desired_folder_name/file.cram`, to create the desired psuedofolder. Biodata Catalyst users who are importing data from Gen3 also have no use for this notebook.
# 
# This notebook's rare use case is for those who will be transferring files without using gsutil, such as using Terra's UI, which otherwise does not provide a way for users to create psuedofolders.
# 
# This is the only notebook in the BYOD notebook suite that is meant to be run *before* actually moving your files into your bucket. All other provided notebooks are meant to be run *after* the files are in place.

# ## A little more info on "psuedofolders"
# This explanation is only given to explain why there's no `mkdir` for GCS and why a notebook is required for what would otherwise be a simple task. It is optional reading.
# 
# Essentially, Google Cloud Storage does not store files in folders. Everything in your bucket is in the same directory. However, some platforms (such as Terra) want to use something like folders for organization and because a folder-based filesystem is what most people are used to working with. 
# So, when Terra encounters something with this filename in Google Cloud Storage:
# `gs://fc-secure-82547374-8637-40cd-aae9-4fae154291d3/test_files/sample.txt`
# Rather than reporting the truth, ie, that a file named `test_files/sample.txt` exists in `gs://fc-secure-82547374-8637-40cd-aae9-4fae154291d3`, it shows the user a "folder" named `test_files` and within that "folder" exists `sample.txt`. So when we say `test_files` is a psuedofolder, that's what we mean. For all intents and purposes, you don't need to worry about this, as both Google Cloud Storage and Terra will "act" as if these are actual folders. So any scripts you have that work on a folder basis, such as the ones used in Paternity Test and other notebooks in this workspace, will work just as you expect.

# # Imports

# In[ ]:


import os
from firecloud import fiss
from firecloud.errors import FireCloudServerError
import firecloud.api as fapi


# # Environmental variables

# In[ ]:


BILLING_PROJECT_ID = os.environ['GOOGLE_PROJECT']
WORKSPACE = os.path.basename(os.path.dirname(os.getcwd()))
BUCKET = os.environ["WORKSPACE_BUCKET"]


# # User-Defined variables

# Enter the name of your folder here. Don't use special characters or whitespace. Do not add any slashes to it.

# In[1]:


FOLDER_NAME="test_files"


# # The actual code

# First of all, we check to make sure Firecloud can find your workspace. If so, a folder is created on the "local" "disc" that the Jupyter notebook is running on. A blank placeholder file is placed into this "local" folder. It is then uploaded to Google Cloud Storage. This creates a psuedofolder with the placeholder file inside of it.

# In[ ]:


try:
    response = fapi.list_entity_types(BILLING_PROJECT_ID, WORKSPACE)
    if response.status_code != 200:
        print("Error in Firecloud, check your billing project ID and the name of your workspace.")
    else:
        print("Firecloud has found your workspace!")
        get_ipython().system('mkdir $FOLDER_NAME')
        get_ipython().system('touch $FOLDER_NAME/placeholder')
        get_ipython().system('gsutil cp -r $FOLDER_NAME $BUCKET')
except NameError:
    print("Caught a NameError exception. This may mean the kernal was restarted or you didn't run ",
          "the cells above. Try running the cells above again.")


# Ta-da! You can now find your psuedofolder in the data section of your workspace. It is also accessible from Terra's terminal and Jupyter notebooks. Note that this directory is also used for workflow runs, so you will see a folder here for every time you have run a workflow in this workspace.

# In[ ]:


get_ipython().system('gsutil ls $BUCKET')


# Now, let's peek inside the new psuedofolder itself. This should only have one file in it.

# In[ ]:


directory = BUCKET +"/"+FOLDER_NAME+"/"
get_ipython().system('gsutil ls $directory')


# # Cleanup

# Note that your folder now contains an empty file called "placeholder." **If you delete it and nothing else is present in the psuedofolder, the psuedofolder will no longer exist, ie, there is no such thing as an empty psuedofolder.** If you wish to delete the placeholder file **after** putting other things into the folder, please run this box below:

# In[ ]:


get_ipython().system('gsutil rm $BUCKET/$FOLDER_NAME/placeholder')

