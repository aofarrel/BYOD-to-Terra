#!/usr/bin/env python
# coding: utf-8

# # Introduction
# Google Cloud Storage is a great tool, but it has an odd way of handling folders. That is to say, it doesn't. There is no equivalent of `mkdir` in Google Cloud Storage. Luckily, with a few tricks, we can get around this limitation and create a psuedo-folder in your workspace's data section, helping keep your BYOD files organized.
# 
# So, basically, notebook creates a new folder in your Terra data directory. That's it. That's all it does.

# Version history:
# 
# | v | date | author | notes |
# | --- | --- | --- | --- |
# | 0.9 | April 3rd 2020 | Ash | initial |
# | 1.0 | April 8th 2020 | Ash | now calls environmental variables, fixed notes on kernal restarting, more explanation |

# ## A little more info on "psuedo-folders"
# This explanation is only given to explain why there's no `mkdir` for GCS and why a notebook is required for what would otherwise be a simple task. It is optional reading.
# 
# Essentially, Google Cloud Storage does not store files in folders. Everything in your bucket is in the same directory. However, some platforms (such as Terra) want to use something like folders for organization and because a folder-based filesystem is what most people are used to working with. 
# So, when Terra encounters something with this filename in Google Cloud Storage:
# `gs://fc-secure-82547374-8637-40cd-aae9-4fae154291d3/test_files/sample.txt`
# Rather than reporting the truth, ie, that a file named `test_files/sample.txt` exists in `gs://fc-secure-82547374-8637-40cd-aae9-4fae154291d3`, it shows the user a "folder" named `test_files` and within that "folder" exists `sample.txt`. So when we say `test_files` is a psuedo-folder, that's what we mean. For all intents and purposes, you don't need to worry about this, as both Google Cloud Storage and Terra will "act" as if these are actual folders. So any scripts you have that work on a folder basis, such as the ones used in Paternity Test and other notebooks in this workspace, will work just as you expect.

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


# # User-Defined stuff

# Enter the name of your folder here. Don't use special characters or whitespace. Do not add any slashes to it.

# In[ ]:


FOLDER_NAME="test_files"


# # The actual code

# First of all, we check to make sure Firecloud can find your workspace. If so, a folder is created on the "local" "disc" that the Jupyter notebook is running on. A blank placeholder file is placed into this "local" folder. It is then uploaded to Google Cloud Storage. This creates a psuedo-folder with the placeholder file inside of it.

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


# Ta-da! You can now find your psuedo-folder in the data section of your workspace. It is also accessible from Terra's terminal and Jupyter notebooks. Note that this directory is also used for workflow runs, so you will see a folder here for every time you have run a workflow in this workspace.

# In[ ]:


get_ipython().system('gsutil ls $BUCKET')


# Now, let's peek inside the new psuedo-folder itself. This should only have one file in it.

# In[ ]:


directory = BUCKET +"/"+FOLDER_NAME+"/"
get_ipython().system('gsutil ls $directory')


# # Cleanup

# Note that your folder now contains an empty file called "placeholder." **If you delete it and nothing else is present in the psuedo-folder, the psuedo-folder will no longer exist, ie, there is no such thing as an empty psuedo-folder.** If you wish to delete the placeholder file **after** putting other things into the folder, please run this box below:

# In[ ]:


get_ipython().system('gsutil rm $BUCKET/$FOLDER_NAME/placeholder')

