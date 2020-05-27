# [BDC] BYOD to Terra
 Repo for Bring Your Own Data (BYOD) resources for Terra. Created as part of BioData Catalyst. Currently a work-in-progress, these resources are made to be used on Terra via its Jupyter Notebook integration. The goal of these tools is two-fold: Clearly show how to move your own data into Terra, and index it in a way that running workflows on said data is simple by leveraging Terra's data table features.

 The notebooks are as follows:
  * Folder Maker: Create a "folder" on Terra for you to neatly upload your data into.
  * File Finder: Generate a Terra data table of BYOD files, the simplest use case. Every row represents one file.
  	(ie, if you had 500 CRAMs and 500 CRAIs, your table would have 1000 rows)
  * Paternity Test: Like File Finder, except parent files are linked to child files. Every row represents a parent file.
  	(ie, if you had 500 CRAMs and 500 CRAIs, your table would have 500 rows)

For more information please see the Use Cases section below.

# Installation
Download the iPython notebooks and then reupload them into your Terra workspace. Unfortunately Terra does not allow automatic update of notebooks due to how Workspaces work, so you will need to redownload/reupload manually if you wish to update later.

# Use Cases

## Prior to bringing your data into the system
**Folder Maker** will help keep your data organized by creating a psuedo-folder in Terra's data section for you to upload your data into. Please note that this is OPTIONAL and is included just for organization purposes.

## After importing your data
### Importing one file type (ex: just CRAMs)
Use **File Finder** to make a TSV file out of your data. This TSV file will link to your data's location in Terra and will automatically be uploaded to Terra as a data table. This means you'll be able to easily run analyses on your data. In this situation, every row on the Terra data table you create represents one file and its location. 

If you'd rather your local system do all the heavy lifting, check **tsvify.sh** instead. tsvify.sh does not support Windows.

### Importing more than one file type (ex: CRAMs and their associated CRAI files)

#### Background
Gen3 uses a data structure involving multiple data tables, where some files are considered children of others, and children know who their parents are, but not vice versa. The most common example is CRAM/CRAI, as some tools require CRAI files in order to run properly. Gen3 has seperate tables for CRAM and CRAI files, where the child (CRAI) links to to the row representing their parent in the CRAM table. In other words, a CRAI file exists as a row in the Gen3 table aligned_reads_index, and one its cells links to its associated CRAM file, which exists as a row in the Gen3 data table submitted_aligned_reads.

#### How Paternity Test Works
**Paternity Test** uses a similiar concept as Gen's parent-child data structure, but instead of linking multiple tables, it just uses one. Let's say you wanted to link CRAM and their associated CRAI files. Every row in your final paternity test table would represent a CRAM file and its child CRAI file. Only one table is created. **Also, although Paternity Test works best with one-child-per-parent situation, you can be applied to any arbitrary number of children. This allows you to use Paternity Test to create, for example, a mega table where each row represents a subject (perhaps denoted by a simple text file, or a file containining phenotypic information), plus the subject's associated CRAM file, CRAI file, and VCF file.**