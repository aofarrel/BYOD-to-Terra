#!/usr/bin/env python
# coding: utf-8

# # File Generator
# Debug notebook for generating thousands of test files.

# In[ ]:


import os
from firecloud import fiss
from firecloud.errors import FireCloudServerError
import firecloud.api as fapi
BILLING_PROJECT_ID = os.environ['GOOGLE_PROJECT']
WORKSPACE = os.path.basename(os.path.dirname(os.getcwd()))
BUCKET = os.environ["WORKSPACE_BUCKET"]
for i in range(1000):
    with open('file%0.3d.cram.bogus' %i,'w') as fd:
        fd.write('just another file')
    #with open('file%0.3d.cram' %i,'w') as fd:
        #fd.write('some other text')
get_ipython().system('gsutil cp -r ./file* $BUCKET/thousands')


# In[ ]:


get_ipython().system('ls')

