#!/usr/bin/env python
# coding: utf-8

# # Table Smasher
# 
# Smashes together Gen3 tables with your own data tables. This assumes that you have already run one of the other notebooks in here to get a data table with your own data and have imported data from Gen3 into this workspace; ie, it smashes together existing tables. You cannot smash a table that does not exist.
# 
# Note that this is focused on tables that have a downloadable file, which in Gen3 is marked by the object_ID column in its datatables. Below is a section of such data tables. If the table you are hoping to merge with a BYOD table is marked here as having an object_ID column, you are free to use it.
# 
# The decision was made to exclude tables that don't link to external files because most of those tables have phenotypic data, and harmonizing BYOD phenotypic data with Gen3 phenotypic data is well beyond the scope of what this notebook could possibly hope to predict.

# |        Data Table         | Have object_ID?| Parent                 | File ext |
# |---------------------------|----------------|------------------------|----------|
# | aligned_reads_index       | Y              | submitted_aligned_reads | CRAI     |
# | aliquot                   | N              |           sample        | -        |
# | germline_variation_index  | Y              |simple_germline_variation| CSI      |
# | program                   | N              |    N/A (top level)      | -        |
# | project                   | N              |        program          | -        |
# | read_group                | N              |           aliquot       | -        |
# | reference_file            | Y              |             project     | various  |
# | simple_germline_variation | Y              |submitted_aligned_reads  | VCF      |
# | study                     | N              |         project         | -        |
# | subject                   | N              |         study           | -        |
# | submitted_aligned_reads   | Y              |      read_group         | CRAM     |
# 

# We will first be going over a use case in which one Gen3 table is combined with one BYOD table.

# # Setup

# In[ ]:


get_ipython().system('pip install tenacity')


# You may have to restart the kernal after installing tenacity.

# ## Run terra-data-utils

# Note to developers: If you change terra_data_util_update.ipynb, you may notice unexpected behavior even if you re-run this cell after making changes to terra_data_util_update.ipynb. To resolve this, try running the cell below two more times. If it still doesn't update, go to your edited notebook and try running the cell you changed. Even if it errors out (such as you not running an import cell it relies upon earlier), return back here and *now* it should update correctly here.

# In[ ]:


#Run the companion notebook. Note: it must be in the same workspace you are currently working in.
get_ipython().run_line_magic('run', 'terra_data_util_update.ipynb')


# ## User-set variables

# In[ ]:


# Don't forgot the quotation marks!
BYOD_TABLE="CRAMs"
OUTPUT_TABLE="craipilation" #Do not include spaces or weird characters


# ## Environmental variables

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
except NameError:
    print("Caught a NameError exception. This may mean the kernal was restarted or you didn't run ",
          "the cells above. Try running the cells above again.")
    raise


# ## Check data table importing

# This will print out out a datatable respresenting the "program" table in Terra from Gen3. Assuming you imported only one TOPMed project, this should be just one row (plus the header row).

# In[ ]:


try:
    gen3df = get_gen3_terra_table_to_df(BILLING_PROJECT_ID, WORKSPACE, "program")
except FireCloudServiceException:
    print("Caught an exception. Check the Data section of this workspace and make sure there is ",
          "a data table called ''program'' that was imported from Gen3.")
    raise
try:
    byod = get_gen3_terra_table_to_df(BILLING_PROJECT_ID, WORKSPACE, BYOD_TABLE)
    print("Successfully found a Gen3 table and a BYOD table.")
except FireCloudServiceException:
    print("We successfully called the Program data table but not the BYOD one. Make sure your ",
          "BYOD table actually exsits and its name has been set correctly as BYOD_TABLE.")
    raise


# # Combine one data BYOD data table with one Gen3 data table
# 
# If you're only working with one type of file, such as CRAMs, it isn't worth your time (real and computational) to combine all of your Gen3 data into one massive table.
# 
# For example: Let's say we want to combine a BYOD datatable of CRAMs that has the columns CRAMs_entity_id and CRAMs_file_location which represent the filename and gs:// location of your files respectively. In Terra's data view, it looks like this:
# 
# | CRAMs_id | file_location |
# |-------|------------|
# | cram1 | gs://cram1 |
# | cram2 | gs://cram2 |
# | cram3 | gs://cram3 |
# | cram4 | gs://cram4 |
# 
# 
# Gen3's data tables follow a consistent format, so we already know the table containing CRAMs will be called submitted_aligned_reads with filenames in the column submitted_aligned_reads_file_name and DRS URIs in the column submitted_aligned_reads_object_id. If we ignore most of the metadata columns, which are irrelevant to the vast majority of workflows, the Gen3 table looks like this:
# 
# | file_name | object_id |submitted_aligned_reads_project_id
# |-------|------------| ----------|
# | cramA | drs://cramA |TOPMED-sample |
# | cramB | drs://cramB |TOPMED-sample |
# | cramC | drs://cramC |TOPMED-sample |
# | cramD | drs://cramD |TOPMED-sample |
# 
# As you can see, both the BYOD table and the Gen3 table have a column representing the file name, and a column representing where that file lives. Therefore, combining those tables will give us something like this:
# 
# | filename | location |project
# |-------|------------| ----------|
# | cram1 | gs://cram1 | N/A |
# | cram2 | gs://cram2 | N/A |
# | cram3 | gs://cram3 | N/A |
# | cram4 | gs://cram4 | N/A |
# | cramA | drs://cramA |TOPMED-sample |
# | cramB | drs://cramB |TOPMED-sample |
# | cramC | drs://cramC |TOPMED-sample |
# | cramD | drs://cramD |TOPMED-sample |

# ## Choose your file type
# Set FILETYPE to either 0, 1, 2, or 3.
# 
# * 0 --> CRAMs   --> will combine with Gen3 table named `submitted_aligned_reads`
# * 1 --> CRAIs   --> will combine with Gen3 table named `aligned_reads_index`
# * 2 --> VCFs    --> will combine with Gen3 table named `simple_germline_variation`
# * 3 --> CSIs    --> will combine with Gen3 table named `germline_variation_index`

# In[ ]:


FILETYPE = 3

filetype_list = ["submitted_aligned_reads", "aligned_reads_index", 
                "simple_germline_variation", "germline_variation_index"]


# In[ ]:


# Call the BYOD datatable we made with File Finder earlier
byod = get_gen3_terra_table_to_df(BILLING_PROJECT_ID, WORKSPACE, BYOD_TABLE)

# Rename the columns
byod = byod.rename(columns={BYOD_TABLE+"_entity_id":"filename", BYOD_TABLE+"_file_location":"location"})
if "filename" in list(byod.columns.values):
    if "location" in list(byod.columns.values):
        print("Successfully set up BYOD dataframe")
    else:
        print("Found data table but failed to rename filename column.",
              "Check names of columns on BYOD Terra data table using",
              "print(byod)")
        raise NameError
else:
    print("Found data table but failed to rename file_location column.",
          "Check names of columns on BYOD Terra data table using",
          "print(byod)")
    raise NameError


# ## Call Gen3 Dataframe

# In[ ]:


gen3df = get_gen3_terra_table_to_df(BILLING_PROJECT_ID, WORKSPACE, filetype_list[FILETYPE])


# ### Add a metadata column (optional)
# It might be useful to keep track of which data comes from where. This can be done by eye by just checking which rows are empty to the column "project" or by seeing which rows have gs:// URIs instead of drs:// URIs. But for some use cases it might be simplier to just add our own metadata in the form of a column. Once we perform the merge, our table will look a bit like this:
# 
# | filename | location |project | source |
# |-------|------------ | ---------| ---- |
# | cram1 | gs://cram1  | N/A | byod |
# | cram2 | gs://cram2  | N/A | byod |
# | cram3 | gs://cram3  | N/A | byod |
# | cram4 | gs://cram4  | N/A | byod |
# | cramA | drs://cramA |TOPMED-sample | gen3 |
# | cramB | drs://cramB |TOPMED-sample | gen3 |
# | cramC | drs://cramC |TOPMED-sample | gen3 |
# | cramD | drs://cramD |TOPMED-sample | gen3 |

# In[ ]:


source = ["byod" for x in range(byod.shape[0])]
byod.insert(2, "source", source)
source = ["gen3" for x in range(gen3df.shape[0])]
gen3df.insert(2, "source", source)


# ## Perform the merge

# In[ ]:


if FILETYPE == 0:
    gen3df = gen3df.rename(columns={"submitted_aligned_reads_file_name":"filename", 
                                            "submitted_aligned_reads_object_id":"location"})
if FILETYPE == 1:
    gen3df = gen3df.rename(columns={"aligned_reads_index_file_name":"filename", 
                                            "aligned_reads_index_object_id":"location"})
if FILETYPE == 2:
    gen3df = gen3df.rename(columns={"simple_germline_variation_file_name":"filename", 
                                            "simple_germline_variation_object_id":"location"})
if FILETYPE == 3:
    gen3df = gen3df.rename(columns={"germline_variation_index_file_name":"filename", 
                                            "germline_variation_index_object_id":"location"})

dfs = [byod, gen3df]
merged = pd.concat(dfs, sort=True)


# Nice! Now we have have both of our dataframes together as one. We can verify by checking the size of each dataframe.

# In[ ]:


if byod.shape[0] + gen3df.shape[0] == merged.shape[0]:
    print("Great success!")
else:
    raise TypeError("Error - row mismatch. Check the structure of your database from the previous step.")


# You can also inspect the dataframe by printing it.

# In[ ]:


print(merged)


# ## Cleanup Dataframe (Optional)
# Terra data tables come with a lot of metadata. Your files probably don't. The vast majority of workflows ignore these metadata columns, so we've provided code to remove them. This section is of course optional and you should skip it if you need every column in your final tables.

# In[ ]:


which_columns = [
    ['submitted_aligned_reads_created_datetime',
               'submitted_aligned_reads_data_category', 'submitted_aligned_reads_data_format',
               'submitted_aligned_reads_data_type', 'submitted_aligned_reads_experimental_strategy',
               'submitted_aligned_reads_file_state', 'submitted_aligned_reads_file_size',
               'submitted_aligned_reads_state','submitted_aligned_reads_updated_datetime'],
    ['aligned_reads_index_created_datetime',
               'aligned_reads_index_data_category', 'aligned_reads_index_data_format',
               'aligned_reads_index_data_type', 'aligned_reads_index_updated_datetime',
               'aligned_reads_index_file_state', 'aligned_reads_index_file_size',
               'aligned_reads_index_state', 'aligned_reads_index_submitter_id'],
    ['simple_germline_variation_created_datetime', 'simple_germline_variation_submitter_id',
               'simple_germline_variation_data_category', 'simple_germline_variation_data_format',
               'simple_germline_variation_data_type', 'simple_germline_variation_experimental_strategy',
               'simple_germline_variation_file_state', 'simple_germline_variation_file_size',
               'simple_germline_variation_state','simple_germline_variation_updated_datetime'],
    ['germline_variation_index_created_datetime',
               'germline_variation_index_data_category', 'germline_variation_index_data_format',
               'germline_variation_index_data_type', 'germline_variation_index_updated_datetime',
               'germline_variation_index_file_state', 'germline_variation_index_file_size',
               'germline_variation_index_state', 'germline_variation_index_submitter_id']
]

merged.drop(columns=which_columns[FILETYPE],inplace=True)


# ## Upload Dataframe to Terra
# Now, let's upload our dataframe to Terra as a data table.

# In[ ]:


get_ipython().system('rm merged.tsv')
# ^ Uncomment above line if you will be running this block more than once

# Reset index, which currently likely has repeats due to how dataframe merging works
merged.reset_index(drop=True, inplace=True)
# Save dataframe to a TSV (uses same function as CSV but with different sep)
merged.to_csv("dataframe.tsv", sep='\t')

# Format resulting TSV file to play nicely with Terra 
# Read the TSV you just made
with open('dataframe.tsv', "r+") as file1:
    # Save the first line as a string
    header = file1.readline()
    # Save everything else as a different variable
    everything_else = file1.readlines()
    file1.close()
# Make the header string Terra requires
full_header="entity:"+OUTPUT_TABLE+"_id"+header

# Make a new TSV
with open('merged.tsv', "a") as file2:
    # Give it the header Terra requires
    file2.write(full_header)
    # Slap in everything else
    for string in everything_else:
        file2.write(string)
    file2.close()

# Upload
response = fapi.upload_entities_tsv(BILLING_PROJECT_ID, WORKSPACE, "merged.tsv", "flexible")
fapi._check_response_code(response, 200)

# Clean up
get_ipython().system('rm dataframe.tsv')

get_ipython().system(' gsutil cp merged.tsv $BUCKET')


# # Combine all datatables

# In[ ]:


logger.setLevel(logging.WARNING)
consolidate_gen3_geno_tables("gen3_data")


# ### Important note on assumptions made
# This notebook was designed to form a datatable where each row represents one subject, and each cell in a given row is metadata for a certain type of TOPMed data for the row's subject. This means things get a bit awkward if your TOPMed data isn't 1-to-1.
# 
# For this reason, simple_germline_variation and germline_variation_index are EXCLUDED from a genotypic merge. This is because we believe study/cohort level VCFs to be more useful to researchers, and because some TOPMed studies do not have VCF/CSI for every CRAM, resulting in some empty cells in a dataframe.
# 
# If your data has more than one sample taken per subject, only one of those samples (and resulting CRAM files) will be put into the dataset. Consider the following situation: You have imported the following tables from TOPMed, diand see the following tables in your Data section...
# * aliquot (12)
# * program (1)
# * project (1)
# * read_group (12)
# * sample (12)
# * study (1)
# * subject (3)
# * subject_set (1)
# * submitted_aligned_reads (12)
# 
# In this situation, there are only three subjects, and each subject had four samples taken. Each of these samples were used to generate a CRAM file.
# 
# Tablesmasher will only select one CRAM (and sample, and aliquot, etc) to represent each subject. This CRAM will be the first one selected. You will know this is going on because when running the function below, because a warning will tell you that 9 duplicate entries have been removed. These are duplicates in the sense that the subject fields are duplicates, even though the CRAMs might be different.
# 
# A different mismatch can occur even before consolidating on subjects. Consider these tables:
# * aliquot (1000)
# * germline_variation_index (950)
# * program (1)
# * project (1)
# * sample (1000)
# * simple_germline_variation (950)
# * subject (999)
# * submitted_aligned_reads (1000)
# 
# Not only does it look like someone got sequenced twice, there's 50 less VCF (submitted_aligned_reads) and CSI (germline_variation_index) files than there are CRAMs (submitted_aligned_reads). In this case, you will see two warnings -- first of all, you will be notified of the possibility of empty cells, because some CRAMs will not be matched to VCF and CSI files. You'll be told there is expected to be 50 empty cells. Then, you will be notified by the dropping of one of the rows, due to a subject getting sequenced twice. So, your final datatable will have 99 rows.

# In[ ]:





# Set the BYOD cram links to have column name of submitted_aligned_reads_object_id, then combine on that column name for the other df.

# In[ ]:





# so one of the problems implict here is your BYOD tables look like one of three cases:
# 
# ### Case A --> supported for one Gen3 merge only, not sure if should support mega Gen3
# 
# | name | URI |
# |-------|------------|
# | cram1 | gs://cram1 |
# | cram2 | gs://cram2 |
# | cram3 | gs://cram3 |
# | cram4 | gs://cram4 |
# 
# Table created by File Finder.
# All files are of one type. 
# 
# ERGO:
# * If merged with one Gen3 table: Not a problem as long as the table is of the same file type (in this case CRAMs)
# * If merged with mega Gen3 table: Will result in empty rows as BYOD is only of one type
# 
# ### Case B --> will not be supported
# | name | URI |
# |-------|------------|
# | cram1 | gs://cram1 |
# | crai1 | gs://crai1 |
# | cram2 | gs://cram2 |
# | crai2 | gs://crai2 |
# 
# Table created by File Finder.
# Files can be of many types, with one file type per row.
# 
# ERGO:
# * If merged with one Gen3 table: Probably impossible
# * If merged with mega Gen3 table: Probably impossible
# 
# *Side note: This is an indication that the arbitarary # of children method of Paternity Test (see below) may be necessary -- merging more than one BYOD type with a Gen3 data table.*
# 
# ### Case C --> should be supported ideally?
# | parent | URI | child1 | child1 URI |
# |--------|------------|--------|------------|
# | cram1 | gs://cram1 | crai1 | gs://crai1 |
# | cram2 | gs://cram2 | crai2 | gs://crai2 |
# | cram3 | gs://cram3 | crai3 | gs://crai3 |
# 
# Table created by Paternity Test.
# One parent and an arbitrary number of children per row.
# 
# ERGO:
# * If merged with one Gen3 table: Would result in empty rows
# * If merged with mega Gen3 table: Would be a total pain to program

# In[ ]:




