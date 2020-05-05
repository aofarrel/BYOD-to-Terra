#!/usr/bin/env python
# coding: utf-8

# # Gen3/Terra Data Utility Functions <a class="tocSkip">

# **Version:**  
# **Status:** This is Notebook is currently a **work in progress** and is not ready for general availability/use quite yet.

# ### Notes from Ash

# The following changes have been made:
# * Users are warned in the case of row mismatch
# * simple_germline_variation and germline_variation_index are excluded from the list of genotypic tables to merge upon (WIP)

# # Purpose
# 
# This Notebook combines multiple Gen3 graph-structured data tables to create a single consolidated table that is easier to use.
# 
# The default behavior is to produce a table keyed by subject id, with one row per subject, for all subjects in a Terra Workspace. This table may include the genomic data, harmonized clinical metadata, or both, along with the associated administrative information.
# The content of the consolidated table produced is configurable.

# # Requirements and Assumptions
# 
# **Run in Terra**  
# This Notebook is intended to be used within the Terra Notebook environment using a Python 3 Jupyter kernel. 
# 
# **Workspace Data**   
# The consolidation is performed for all Gen3 data for the BioData Catalyst program in a Terra workspace. The data may be for subjects from one or more projects/cohorts.
# 
# **Libraries**   
# The following libraries are expected to be available in the Terra Notebook environment, either by being preinstalled the 'Terra Notebook Runtime, Container Image, or explicit installation by the user:
# * `fiss` (version 0.16.23 or later)
# * `numpy` (version 1.15.2 or later)
# * `pandas` (0.25.3 or laster)
# * `tenacity` (6.1.0 or later)
# 
# **Global Variable Settings**
# Currently, the following global variables are required to be set prior to calling the functions in this Notebook:
# * `BILLING_PROJECT_ID`
# * `WORKSPACE` 

# # How to Use
# 
# The recommended way to use this Notebook is to "import" this Notebook into a user's primary Notebook using the `%run` command. The following steps added to a user's primary Notebook is sufficient to create a consolidated data table, in this example, a consolidated table containing both the genomic data and harmonized metadata:
# 
# ````
# %run terra_data_util.ipynb  
# 
# BILLING_PROJECT_ID = os.environ['GOOGLE_PROJECT']  
# WORKSPACE = os.environ['WORKSPACE_NAME']
# 
# consolidate_gen3_geno_pheno_tables("consolidated_metadata")
# ```
# 
# This Notebook provides the following ready-to-use functions for creating consolidated tables:
# * `consolidate_gen3_geno_pheno_tables(new_table_name: str)`
# * `consolidate_gen3_geno_tables(new_table_name: str)`
# * `consolidate_gen3_pheno_tables(new_table_name: str)`
# 
# A convenience function to delete Terra data tables, a time-consuming process, is also included:
# * `delete_terra_table(project: str, workspace: str, table_name: str)`
# 
# The Terra data tables that are included in the consolidated table, and how they are combined, is defined by a merge specification defined as a Python dictionary.
# The merge specification supports standard SQL-style join operations and can be customized as desired.

# # How it Works
# 
# This Notebook uses the Broad FireCloud API to read each Terra data table identified in the merge specification into a Pandas DataFrame and performs SQL-style joins on the tables using the Pandas `merge` operation to produce a single, consolidated table.
# References in the Gen3 data model are only the direction of the graph leaf/bottom nodes to the root/top node.
# 
# During this consolidation process, the name of each column in a table is prefixed with the name of the table it is from. Additionally, the columns containing entity ids have the `_entity_id` suffix appended.

# # Dependencies and Imports

# Ensure that a recent version of firecloud is installed.
# The version must be 0.16.23 or later for flexible entity support.

# In[ ]:


# ! pip install --upgrade firecloud
# ! pip show firecloud
# ! pip install tenacity
# ! pip install pysnooper


# In[ ]:


import io
import os
import sys
from datetime import datetime
import json

from firecloud import fiss
from firecloud.errors import FireCloudServerError
import firecloud.api as fapi
import numpy as np
import pandas as pd
import tenacity
from tenacity import retry, after_log, before_sleep_log, retry_if_exception_type, stop_after_attempt, wait_exponential


# In[ ]:


import logging
from logging import INFO, DEBUG
logger = logging.getLogger()
logger.setLevel(INFO)


# # Commonly Used Merge Specifications and Convenience Functions

# ## Create a consolidated data table containing both genomic and phenotypic data

# In[ ]:


GEN3_GENO_PHENO_MERGE_SPEC = {
  "default_join_type": "outer",
  "merge_sequence": [
    # Commented out as this is rarely used by researchers and can lead to weird data table shenanigans
    #{
      #"join_column": "simple_germline_variation",
      #"table_names": ["simple_germline_variation", "germline_variation_index"]
    #},
    {
      "join_column": "submitted_aligned_reads",
      "table_names": ["submitted_aligned_reads", "aligned_reads_index"]
    },
    {
      "join_column": "read_group",
      "table_names": ["read_group", "submitted_unaligned_reads", "read_group_qc"]
    },
    {
      "join_column": "aliquot",
      "table_names": ["aliquot", "submitted_cnv_array", "submitted_snp_array"]
    },
    {
      "join_column": "sample",
      "table_names": ["sample"]
    },
    {
      "join_column": "subject",
      "table_names": ["subject", "blood_pressure_test", "cardiac_mri", "demographic", "electrocardiogram_test", "exposure", "lab_result", "medical_history", "medication"]
    },
    {
      "join_column": "study",
      "table_names": ["study"]
    },
    {
      "join_column": "project",
      "table_names": ["project"]
    },
    {
      "join_column": "program",
      "table_names": ["program"]
    }
  ],
  "final_index_source_column": "subject_submitter_id"
}


# In[ ]:


def consolidate_gen3_geno_pheno_tables(new_table_name: str):
    consolidate_to_terra_table(GEN3_GENO_PHENO_MERGE_SPEC, new_table_name)


# ## Create a consolidated data table containing only genomic (no phenotypic) data

# In[ ]:


GEN3_GENO_MERGE_SPEC =  {
  "default_join_type": "outer",
  "merge_sequence": [
    # Commented out as this is rarely used by researchers and can lead to weird data table shenanigans
    #{
      #"join_column": "simple_germline_variation",
      #"table_names": ["simple_germline_variation", "germline_variation_index"]
    #},
    {
      "join_column": "submitted_aligned_reads",
      "table_names": ["submitted_aligned_reads", "aligned_reads_index"]
    },
    {
      "join_column": "read_group",
      "table_names": ["read_group", "submitted_unaligned_reads", "read_group_qc"]
    },
    {
      "join_column": "aliquot",
      "table_names": ["aliquot", "submitted_cnv_array", "submitted_snp_array"]
    },
    {
      "join_column": "sample",
      "table_names": ["sample"]
    },
    {
      "join_column": "subject",
      "table_names": ["subject"]
    },
    {
      "join_column": "study",
      "table_names": ["study"]
    },
    {
      "join_column": "project",
      "table_names": ["project"]
    },
    {
      "join_column": "program",
      "table_names": ["program"]
    }
  ],
  "final_index_source_column": "subject_submitter_id"
}


# In[ ]:


def consolidate_gen3_geno_tables(new_table_name: str):
    consolidate_to_terra_table(GEN3_GENO_MERGE_SPEC, new_table_name)


# ## Create a consolidated data table containing only phenotypic (not genomic) data

# Note: Here the "sample" table is being included in the phenotypic data because it contains useful identifier information (e.g., the "NWD" identifier).

# In[ ]:


GEN3_PHENO_MERGE_SPEC =  {
  "default_join_type": "outer",
  "merge_sequence": [
    {
      "join_column": "subject",
      "table_names": ["subject", "sample", "blood_pressure_test", "cardiac_mri", "demographic", "electrocardiogram_test", "exposure", "lab_result", "medical_history", "medication"]
    },
    {
      "join_column": "study",
      "table_names": ["study"]
    },
    {
      "join_column": "project",
      "table_names": ["project"]
    },
    {
      "join_column": "program",
      "table_names": ["program"]
    }
  ],
  "final_index_source_column": "subject_submitter_id"
}


# In[ ]:


def consolidate_gen3_pheno_tables(new_table_name: str):
    consolidate_to_terra_table(GEN3_PHENO_MERGE_SPEC, new_table_name)


# # Custom Merge Specification and Use
# 
# TODO - Information about customizing merge specifications is needed, and will likely be fairly volumonous. This may be best placed in a repo readme file.

# In[ ]:


GEN3_USER_CUSTOM_MERGE_SPEC =  {
  "default_join_type": "inner",
  "merge_sequence": [
    # Commented out as this is rarely used by researchers and can lead to weird data table shenanigans
    #{
      #"join_column": "simple_germline_variation",
      #"table_names": ["simple_germline_variation", "germline_variation_index"]
    #},
    {
      "join_column": "submitted_aligned_reads",
      "table_names": ["submitted_aligned_reads", "aligned_reads_index"]
    },
    {
      "join_column": "read_group",
      "table_names": ["read_group", "submitted_unaligned_reads", "read_group_qc"]
    },
    {
      "join_column": "aliquot",
      "table_names": ["aliquot", "submitted_cnv_array", "submitted_snp_array"]
    },
    {
      "join_column": "sample",
      "table_names": ["sample"]
    },
    {
      "join_column": "subject",
      "join_type": "left",
      "table_names": ["subject", "blood_pressure_test", "cardiac_mri", "demographic", "electrocardiogram_test", "exposure", "lab_result", "medical_history", "medication"]
    },
    {
      "join_column": "study",
      "table_names": ["study"]
    },
    {
      "join_column": "project",
      "table_names": ["project"]
    },
    {
      "join_column": "program",
      "table_names": ["program"]
    }
  ],
  "final_index_source_column": "subject_submitter_id"
}


# In[ ]:


def consolidate_gen3_custom_tables(new_table_name: str):
    consolidate_to_terra_table(GEN3_USER_CUSTOM_MERGE_SPEC, new_table_name)


# # Related Convenience Functions

# Perform the merges defined in the specification and writes the resulting table to the given name.

# In[ ]:


def consolidate_to_terra_table(merge_spec: dict, entity_name: str)  -> pd.DataFrame:
    
    if 'final_index_source_column' in merge_spec and len(merge_spec['final_index_source_column']):
        entity_id_column = merge_spec['final_index_source_column']
    else:
        logger.error("The merge specification field \"final_index_source_column\" is missing or has an empty value.")
        return
    
    # Check for an existing table with the same name and log accordingly
    if (entity_name in DataTableInfo.get_table_names()):
        existing_rows, existing_columns, _ = DataTableInfo.get_table_info(entity_name)
        logger.info("A data table with the name \"{}\" already exists with dimmesions ({}x{}). Corresponding data will be updated and any existing additional data will be left unchanged.".format(
        entity_name, existing_rows, existing_columns))
    
    consolidated_df = consolidate_to_df(merge_spec)
 
    # Add "entity:{entity_name}_id" as the first column, as required by Terra.
    consolidated_df.insert(0, f"entity:{entity_name}_id", consolidated_df[entity_id_column])
    
    consolidated_df_rows, consolidated_df_columns = consolidated_df.shape
    if logger.isEnabledFor(DEBUG):
        logger.info("The in-memory consolidated data frame size is: {} rows x {} columns".format(consolidated_df.shape[0], consolidated_df.shape[1]))
        write_df_to_tsv_file(consolidated_df, "consolidated_df")
    
    upload_entities_df(consolidated_df)
    
    # Compare the in-memory and actual uploaded data table sizes and output the results.
    actual_rows, actual_columns, _ = DataTableInfo.get_table_info(entity_name, True)
    if (consolidated_df_rows == actual_rows and consolidated_df_columns == actual_columns):
        logger.info("The consolidated data table \"{}\" size is: {} rows x {} columns".format(
            entity_name, actual_rows, actual_columns))
    else:
        if (consolidated_df_rows > actual_rows or consolidated_df_columns > actual_columns):
            logger.error("Data table truncation error."
                         " The in-memory consolidated data table has more rows or columns ({}x{}) than the data table \"{}\" uploaded to Terra ({}x{})".format(
                           consolidated_df_rows, consolidated_df_columns, entity_name, actual_rows, actual_columns))
        else:
            logger.warning("Data table size mismatch warning."
                           " The in-memory consolidated data table has fewer rows or columns ({}x{}) than the data table \"{}\" uploaded to Terra ({}x{})".format(
                           consolidated_df_rows, consolidated_df_columns, entity_name, actual_rows, actual_columns)) 
            
    if logger.isEnabledFor(DEBUG):
        all_tables_info = json.dumps(DataTableInfo.get_data_table_info(), indent=4)
        logger.debug("All table info: {}".format(all_tables_info))
            


# Create a Pandas DataFrame containing the contents of the given Terra data table.

# In[ ]:


@retry(reraise=True,
       stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1, min=2, max=10),
       after=after_log(logger, logging.DEBUG),
       before_sleep=before_sleep_log(logger, logging.INFO))
def get_terra_table_to_df(project: str, workspace: str, table_name: str, attributeNames=None, model="flexible") -> pd.DataFrame:
    response = fapi.get_entities_tsv(project, workspace, table_name, attributeNames, model=model)
    if response.status_code != 200:
        raise FireCloudServiceException(response.status_code, str(response.content) + " Error code: " + str(response.status_code))
    
    table_df = pd.read_csv(io.StringIO(response.text), sep='\t')
    
    # Change the dataframe index from the default numeric index to the the entity id column.
    # TODO - Resetting the index below had the unexpected effect of causing the subsequent merge
    #        operation to fail due to a key error, even though the intended key was present
    #        in both tables. Omit the following until it can be investigated and resolved.
    # table_df.set_index(f"entity:{table_name}_id", inplace=True)
    
    return table_df


# Create a Pandas DataFrame containing the contents of the given Terra data table, with columns renamed to facilitate merging:
# * In general, column names are prefixed with the name of the table to address conflicts that would otherwise occur due to fields having the same name in multiple different tables.  
# * Columns representiong relationships between tables are suffixed with `_entity_id`.

# In[ ]:


def get_gen3_terra_table_to_df(project: str, workspace: str, table_name: str, model="flexible") -> pd.DataFrame:
    table_df = get_terra_table_to_df(project, workspace, table_name)
    columns = table_df.columns
    rename_column(table_df, f"entity:{table_name}_id", f"{table_name}_entity_id") # Column 0
    for column in columns[1:]:
        if column in GEN3_TABLE_NAMES:
            rename_column(table_df, column, f"{column}_entity_id")
        else:
            rename_column(table_df, column, f"{table_name}_{column}")
    # Deduplicate "*_entity_id" columns
    table_df = table_df.loc[:,~table_df.columns.duplicated()]
    return table_df


# Rename a column in the given Pandas DataFrame.

# In[ ]:


def rename_column(df: pd.DataFrame, current_column_name: str, new_column_name: str) -> None:
    df.rename(columns={current_column_name : new_column_name}, inplace=True)


# Upload the contents of the Pandas DataFrame to a Terra data table.  
# This includes support for "chunking" large tables into smaller sections that can be successfully uploaded individually.
# 
# Note: The format of the table within the Pandas DataFrame must comform to the format described here: https://support.terra.bio/hc/en-us/articles/360025758392-Managing-data-with-tables-

# In[ ]:


def upload_entities_df(df: pd.DataFrame, chunk_size=500) -> None:
    logger.info("Starting upload of data table to Terra. This may require serveral minutes or longer for large tables.")
    chunk_start = chunk_end = 0
    row_count = df.shape[0]
    output_now("Uploading ")
    while chunk_start < row_count:
        chunk_end = min(chunk_start + chunk_size, row_count)
        chunk_df = df.iloc[chunk_start:chunk_end]
        chunk_tsv = chunk_df.to_csv(sep="\t", index=False)
        fapi_upload_entities(BILLING_PROJECT_ID, WORKSPACE, chunk_tsv, "flexible")
        chunk_start = chunk_end
        output_now(".")
    output_now("\n")
    logger.info("Finished upload of data table to Terra.")


# Delete the Terra data table with the given billing project, workspace and name.

# In[ ]:


def delete_terra_table(project: str, workspace: str, table_name: str):
    if table_name not in DataTableInfo.get_table_names(True):
        logger.warning("Data table \"{}\" not found.".format(table_name))
        return
    
    logger.info("Starting deletion of data table \"{}\". This may require serveral minutes or longer for large tables.".format(table_name))
    # TODO There should be better way than this to simply delete a table/entity-type.
    entity_id_column_name = f"entity:{table_name}_id"
    table_to_delete_df = get_terra_table_to_df(project, workspace, table_name, attributeNames=[entity_id_column_name])
    entity_id_series = table_to_delete_df[entity_id_column_name]
    num_chunks = entity_id_series.size / 100
    output_now("Deleting ")
    for chunk in  np.array_split(entity_id_series, num_chunks):
        response = fapi_delete_entity_type(project, workspace, table_name, chunk)
        output_now(".")
    output_now("\n")
    logger.info("\nFinished deleting data table \"{}\".".format(table_name))    


# Delete all Gen3 data tables in the given billing project, workspace and name.

# In[ ]:


def delete_all_gen3_tables(project: str, workspace: str):
    logger.info("Deleting all Gen3 tables in workspace \"{}\". This may require a very long time depending on the number and size of the Gen3 tables.".format(workspace))
    # TODO Prompt for user confirmation before proceeding.
    DataTableInfo.refresh()
    for gen3_table_name in GEN3_TABLE_NAMES:
        if gen3_table_name in DataTableInfo.get_table_names():
            delete_terra_table(project, workspace, gen3_table_name)
    logger.info("Finished deleting all Gen3 tables in workspace \"{}\".".format(workspace))


# Deletes all Terra tables from the in the given billing-project and workspace.
# 
# BE VERY CAREFUL WITH THIS FUNCTION TO AVOID LOSING VALUABLE DATA!

# In[ ]:


def delete_all_tables(project: str, workspace: str):
    logger.info("Deleting all tables in workspace \"{}\". This may require a very long time depending on the number and size of the Gen3 tables.".format(workspace))
    # TODO Prompt for user confirmation before proceeding!
    DataTableInfo.refresh()
    for table_name in DataTableInfo.get_table_names():
        delete_terra_table(project, workspace, table_name)
    logger.info("Finished deleting all tables in workspace \"{}\".".format(workspace))


# # Internals

# Data and functions used internally and not intended for user modification.  
# *The code in the rest of this document will likely be moved to a new Python library "soon".*

# This is the set of tables defined in the Gen3 data model, for Notebook-internal use.  
# All of the tables used in merge specications must exist in this set yet this set may contain additional tables names are not used in the merge specifications and do not exist in the current workspace data table.

# In[ ]:


GEN3_TABLE_NAMES={"aligned_reads_index",
                 "aliquot",
                 "blood_pressure_test",
                 "cardiac_mri",
                 "demographic",
                 "electrocardiogram_test",
                 "exposure",
                 "germline_variation_index",
                 "lab_result",
                 "medical_history",
                 "medication",
                 "program",
                 "project",
                 "read_group",
                 "read_group_qc"
                 "reference_file",
                  "reference_file_index",
                 "sample",
                 "simple_germline_variation",
                 "study",
                 "subject",
                 "submitted_aligned_reads",
                 "submitted_cnv_array",
                 "submitted_snp_array",
                 "submitted_unaligned_reads"
                 }


# In[ ]:


if logger.isEnabledFor(DEBUG):
    get_ipython().run_line_magic('xmode', 'Verbose')
    import pysnooper


# In[ ]:


def consolidate_to_tsv(merge_spec: dict)  -> pd.DataFrame:
    return consolidate_to_df(merge_spec).to_csv(sep="\t")


# In[ ]:


# @pysnooper.snoop
def consolidate_to_df(merge_spec: dict)  -> pd.DataFrame:
    default_merge_parameters = merge_spec['default_merge_parameters'] if 'default_merge_parameters' in merge_spec else dict(how="outer")
    if "default_join_type" in merge_spec:
        default_merge_parameters['how'] = merge_spec['default_join_type']
        
    merged_df = None
    previous_row_count = -1
    for merge_info in merge_spec['merge_sequence']:
        merge_parameters = _create_combined_merge_parameters(default_merge_parameters, merge_info)
        _substitute_entity_id_column_name(merge_parameters)
        merged_df = consolidate_tables_to_df(merge_info['table_names'], merge_parameters, merged_df)
        
        # Determine if row count has increased
        if previous_row_count != -1:
            if previous_row_count < merged_df.shape[0]:
                n = merged_df.shape[0]-previous_row_count
                logger.warning("Row count increased unexpectedly, potentially meaning some empty cells. {0} empty cells anticipated.".format(n))
        previous_row_count = merged_df.shape[0]
    
    return merged_df

def _create_combined_merge_parameters(default_merge_parameters: dict, merge_info: dict) -> dict:
    standard_pandas_default_parameters = dict(how="inner", on=None, left_on=None, right_on=None, left_index=False, right_index=False, sort=False, suffixes=("_x", "_y"), copy=True, indicator=False, validate=None)
    combined_parameters = standard_pandas_default_parameters.copy()
    combined_parameters.update(default_merge_parameters)
    if 'merge_parameters' in merge_info:
        combined_parameters.update(merge_info['merge_parameters'])
    if 'join_column' in merge_info:
        combined_parameters['on'] = merge_info['join_column']
    if 'join_type' in merge_info:
        combined_parameters['how'] = merge_info['join_type']
    return combined_parameters

def _substitute_entity_id_column_name(merge_parameters: dict) -> dict:
    for key in 'on', 'left_on', 'right_on':
        if key in merge_parameters and merge_parameters[key]:
            merge_parameters[key] = get_entity_id_column_name(merge_parameters[key])
            # TODO - Add support for case where value is a list/array - requires careful testing


# In[ ]:


# @pysnooper.snoop()
def consolidate_tables_to_df(table_names: list, merge_parameters: dict, initial_df = None) -> pd.DataFrame:
    if initial_df is None:
        assert len(table_names) >= 2, "At least two table names are required." 
        table_name = table_names[0]
        table_names = table_names[1:]
        first_df = get_gen3_terra_table_to_df(BILLING_PROJECT_ID, WORKSPACE, table_name)
        if table_name == "sample":
            _deduplicate_merge_data(None, first_df, "sample", get_entity_id_column_name("subject"))
        merged_df = first_df
    else:
        assert len(table_names) >= 1, "At least one table names is required to merge with previous data."
        merged_df = initial_df
        
    for table_name in table_names:
        
        if table_name not in DataTableInfo.get_table_names():
            logger.info("The table \"{}\" was not found in this workspace and will be ignored.".format(table_name))
            continue            
        current_df = get_gen3_terra_table_to_df(BILLING_PROJECT_ID, WORKSPACE, table_name)
        if table_name == "sample":
            _deduplicate_merge_data(merged_df, current_df, "sample", get_entity_id_column_name("subject"))
            
        if logger.isEnabledFor(DEBUG):
            write_df_to_tsv_file(merged_df, "merged_df")
            write_df_to_tsv_file(current_df, "current_df")
            
        logger.debug("Merging table \"{}\" using column \"{}\" with join type: {}".format(
            table_name, merge_parameters['on'], merge_parameters['how']))
        logger.debug("Full merge parameters: {}".format(merge_parameters))
        
        merged_df = merged_df.merge(current_df, **merge_parameters)
        
        # Deduplicate "*_entity_id" columns
        merged_df = merged_df.loc[:,~merged_df.columns.duplicated()]
        
        logger.info("Merged table \"{}\" using column \"{}\" with join type: \"{}\". New merged table dimmensions: ({}x{})".format(
            table_name, merge_parameters['on'], merge_parameters['how'], merged_df.shape[0], merged_df.shape[1]))
        
    return merged_df


# In[ ]:


def _deduplicate_merge_data(merged_df: pd.DataFrame, current_df: pd.DataFrame,
                           current_table_name: str, current_dedup_key: str) -> None:
    # Some TOPMed projects (COPDGene, MESA, maybe others) are known to have multiple sample
    # entries for the same subject. According to BioData Catalyst data experts,
    # the duplicates should be equivalent, so just keep the first entry found in each case.

    # Identify duplicates in the given column of the current table and obtain
    # a list of entity ids for the rows containing duplicates.
    # Then remove the duplicate rows from the current table.
    current_dups = current_df[current_dedup_key].duplicated(keep="first")
    current_dups_values = current_df[current_dups][current_dedup_key].tolist()
    if len(current_dups_values) == 0:
        logger.debug("No duplicates found in table {} for key {}".format(current_table_name, current_dedup_key))
        return
    current_table_entity_id = get_entity_id_column_name(current_table_name)
    common_key_values_for_dupes = current_df[current_dups][current_table_entity_id].tolist()
    current_df.drop(current_df[current_dups].index, inplace=True)
    logger.warning("Removed {} duplicate entries from table \"{}\" in column \"{}\". Retained the first entry found. Deleted rows with ids: {}".format(
        len(current_dups_values), current_table_name, current_dedup_key, current_dups_values))

    # From the results that have been merged thus far, remove the rows that would have been joined
    # to the rows that were deleted as duplicates from the current table. This will prevent "orphan"
    # rows from being created in the consolidated dataframe, which would otherwise happen with
    # some join types (e.g. "outer").
    if merged_df is not None and current_table_entity_id in merged_df.columns:
        mask = merged_df[current_table_entity_id].isin(common_key_values_for_dupes)
        merged_df.drop(merged_df[mask].index, inplace=True)


# In[ ]:


def get_entity_id_column_name(entity_type: str):
    return f"{entity_type}_entity_id"


# In[ ]:


def write_df_to_tsv_file(df: pd.DataFrame, filename: str) -> None:
    filename += "_" + datetime.now().strftime("%Y%m%d_%H%M%S%f") + ".tsv"
    with open(filename, mode="w") as tsv_file:
        tsv_string = df.to_csv(sep="\t", index=False)
        tsv_file.write(tsv_string)


# In[ ]:


@retry(reraise=True,
       retry=retry_if_exception_type(FireCloudServerError), 
       stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1, min=2, max=10),
       after=after_log(logger, logging.DEBUG),
       before_sleep=before_sleep_log(logger, logging.INFO))
def fapi_upload_entities(project: str, workspace: str, entity_tsv: str, model: str):
    response = fapi.upload_entities(project, workspace, entity_tsv, model)
    fapi._check_response_code(response, 200)


# In[ ]:


@retry(reraise=True,
       retry=retry_if_exception_type(FireCloudServerError), 
       stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1, min=2, max=10),
       after=after_log(logger, logging.DEBUG),
       before_sleep=before_sleep_log(logger, logging.INFO))
def fapi_delete_entity_type(namespace: str, workspace: str, etype: str, ename) -> dict:
    response = fapi.delete_entity_type(namespace, workspace, etype, ename)
    fapi._check_response_code(response, 204)
    return response


# In[ ]:


def output_now(message: str) -> None:
    sys.stderr.write(message)
    sys.stderr.flush()


# In[ ]:


class DataTableInfo:
    _data_table_info = None
    _data_table_names = None

    @classmethod
    def refresh(cls):
        response = fapi.list_entity_types(BILLING_PROJECT_ID, WORKSPACE)
        if response.status_code == 200:
            cls._data_table_info = json.loads(response.text)
            cls._data_table_names = list(cls._data_table_info.keys())
        else:
            cls._data_table_info = None
            cls._data_table_names = None
            raise FireCloudServiceException(
                response.status_code,
                "Failed to get entity types. Error code: {}".format(
                    response.status_code))

    @classmethod
    def get_data_table_info(cls, refresh=False):
        if not cls._data_table_info or refresh:
            cls.refresh()
        return cls._data_table_info.copy()

    @classmethod
    def get_table_names(cls, refresh=False):
        if not cls._data_table_names or refresh:
            cls.refresh()
        return cls._data_table_names.copy()

    @classmethod
    def get_table_info(cls, table_name, refresh=False):
        if not cls._data_table_info or refresh:
            cls.refresh()
        row_count = None
        column_count = None
        attributes = None
        if table_name in cls._data_table_names:
            row_count = cls._data_table_info[table_name]['count']
            attributes = cls._data_table_info[table_name]['attributeNames'].copy()
            column_count = len(attributes) + 1  # Add one for the entity id column
        return row_count, column_count, attributes


# In[ ]:


class FireCloudServiceException(Exception):
    """ A FireCloud service error occurred.

    Attributes:
        code (int): HTTP response code indicating error type
        message (str): Response content, if present
    """
    def __init__(self, code, message):
        self.code = code
        self.message = message
        Exception.__init__(self, message)


# ## Built-in Test/Debug Code

# Uncomment the lines in the cells below to enable some built-in testing, improved debugging abilities or to serve as a simple stand-alone demo.

# To test with data in a different workspace than the one that contains this Notebook,
# specify remote workspace information below. This enables convenient testing of data
# for multiple different projects/cohorts using this same Notebook in the current workspace.

# In[ ]:


# os.environ['GOOGLE_PROJECT'] = os.environ['WORKSPACE_NAMESPACE'] = "anvil-stage-demo"
# os.environ['WORKSPACE_NAME']="mbaumann terra_data_util test Amish"


# Set standard names used in this Notebook for these values.

# In[ ]:


# BILLING_PROJECT_ID = os.environ['GOOGLE_PROJECT']
# WORKSPACE = os.environ['WORKSPACE_NAME']


# ### Specify, create and optionally delete the desired tables

# In[ ]:


# create_example_consolidated_geno_pheno_table=True
# create_example_consolidated_geno_table=True
# create_example_consolidated_pheno_table=True
# create_example_consolidated_custom_table=True
# delete_created_tables=False


# In[ ]:


# if create_example_consolidated_geno_pheno_table:
#     example_table_name = "example_consolidated_geno_pheno_table"
#     consolidate_gen3_geno_pheno_tables(example_table_name)
#     if delete_created_tables:
#          delete_terra_table(BILLING_PROJECT_ID, WORKSPACE, example_table_name)


# In[ ]:


# if create_example_consolidated_geno_table:
#     example_table_name = "example_consolidated_geno_table"
#     consolidate_gen3_geno_tables(example_table_name)
#     if delete_created_tables:
#          delete_terra_table(BILLING_PROJECT_ID, WORKSPACE, example_table_name)


# In[ ]:


# if create_example_consolidated_pheno_table:
#     example_table_name = "example_consolidated_pheno_table"
#     consolidate_gen3_pheno_tables(example_table_name)
#     if delete_created_tables:
#          delete_terra_table(BILLING_PROJECT_ID, WORKSPACE, example_table_name)


# In[ ]:


# if create_example_consolidated_custom_table:
#     example_table_name = "example_consolidated_custom_table"
#     consolidate_gen3_custom_tables(example_table_name)
#     if delete_created_tables:
#          delete_terra_table(BILLING_PROJECT_ID, WORKSPACE, example_table_name)


# In[ ]:


# Delete all tables
# for example_table_name in "example_consolidated_geno_pheno_table", "example_consolidated_geno_table",\
# "example_consolidated_pheno_table", "example_consolidated_custom_table":
#         try:
#             logger.info("Deleting: {}".format(example_table_name))
#             delete_terra_table(BILLING_PROJECT_ID, WORKSPACE, example_table_name)
#             logger.info(f"Finished deleting:{}".format(example_table_name))
#         except Exception as ex:
#             logger.warning("Table {} may not exist.".format(example_table_name))


# In[ ]:




