# Bringing Your Own Data (BYOD) to Terra
## Introduction
"Interoperability" is a term that gets heard a lot these days, but what does it truly mean? In the context of bioengineering, it refers to the ability of different systems to work together. A simple example of this is how workflows written in WDL can be directly imported to Terra from their respective pages on Dockstore. But what if the data you want to run your workflows upon isn't in your Terra workspace yet? Or perhaps you've already imported, but perhaps you are in search of a good way to index it so you can use hundreds of files as a workflow input without typing them manually. If so, you've come to the right place.

The question of bringing your own data (BYOD, as we call it) is a complicated one to write documentation for, as there are so many different places your data may be and so many different things you may wish to do with said data.

We've decided to break down the task into two main sections: First of all, you must get your data into the system in the first place. We call this "Source-to-Bucket," referring to the movement of your files from your BYOD source into the Google Cloud Bucket of your Terra worksapce. Second, once your data is in the system, you will want to learn how to use it in either a Jupyter notebook, or a workflow. We call that part "Bucket-to-Compute."

**Note:** If you wish to import your data in a Jupyter notebook to process it and then pass that output into a workflow, it is easiest to think about it as two separate BYOD problems. The first is for getting your data into your bucket in a way that can be processed by Jupyter. The second is moving your Jupyter output back into your Google Bucket as the Source-to-Bucket, and setting up your data to run as an input of your workflow is the Bucket-to-Compute part.

## Before you begin
### File names
Please keep in mind that any of these characters, if present in your filenames, may cause problems.

|            | Spaces             | Newline/Tab  | Slashes                           | Non-ascii      |
|------------|--------------------|-------------------|-----------------------------------|----------------|
| Might work | "yamada taro".cram |   yamadatabtaro.cram      | "yamada\taro".cram                | 山田太郎.cram  |
| Won't work | yamada taro.cram   | yamada\ntaro.cram | yamada/taro.cram yamada\taro.cram | 山田 太郎.cram |

Please note the "might work" column is **not** a recommendation. The recommendation is for file names to only have the characters A-Z, a-z, 0-9, _, ., and -.

### Keeping your bucket organized
For every Source-to-Bucket situation except for 8 (see below), you will be transferring files into your Terra workspace's data section. You may wish to keep this section organized using psuedofolders (strictly speaking gsutil does not have folders). If you are transferring files to your bucket via gsutil (see below), simply add the desired folder name to your `gsutil cp` command, such as `gsutil cp gs://source/file.cram gs://destination/desired_folder_name/file.cram`, to create the desired psuedofolder.

If you are not using gsutil to transfer your files or want to learn more about psuedofolders, you can make use of [a small Jupyter notebook named Psuedofolder Maker](https://github.com/DataBiosphere/BYOD-to-Terra/blob/anvil/Psuedofolder%20Maker.py) that you can run in Terra to create a "psuedofolder" in your Data section. 

If during the Bucket-to-Compute part of your BYOD project, you will be using one of the Jupyter notebooks provided to create a datatable, it is **strongly** recommended that you make use of psuedofolders, because the Bucket-to-Compute notebooks operate on all files within a given psuedofolder.

## Source-to-Bucket
Depending on where your data is located, your Source-to-Bucket method will vary. Please see the following flow chart for details.
![Flowchart for BYOD situations](https://raw.githubusercontent.com/DataBiosphere/BYOD-to-Terra/anvil/BYOD%20-%20Anvil%20numbered.png)

### Situation 1: Institute server not owned by the Broad
If your data is on an institute server not owned by the Broad, you may be able to adapt one of the other methods presented here. Due to the range of different configurations, we can't predict what will work for every user, but we recommend first trying `gsutil cp` from your server (see Situation 3, treating the server as your local machine).

### Situation 2: Broad Institute server
Information can be transferred directly from a Broad Institute Server to Terra [using the instructions documented in "Option 3" here](https://support.terra.bio/hc/en-us/articles/360024056512-Uploading-to-a-workspace-Google-bucket). This is similar to Situation 6 but has a slightly different setup.

### Situation 3: Local machine, using gsutil
The easiest way to upload multiple files from your local machine to your Terra workspace is to use gsutil on your local machine. gsutil is the tool used for interfacing between Google Cloud Storage buckets.

#### Installation
Please see [Google's instructions for installation](https://cloud.google.com/storage/docs/gsutil_install). gsutil is a command line tool, but unlike many command line tools, there is also a Windows version available.

#### Copying files over
First of all, you need to know the Google Cloud Storage "address" of your Terra workspace. You can find this on the Dashboard page of your workspace, below the tags section. You can click the icon of a clipboard to copy the full bucket address.

![Where to find the google bucket address of your workspace](https://github.com/aofarrel/tutorials/blob/master/google_bucket_resized.png?raw=true)

Next, use `gsutil cp` to move the files to your workspace bucket. Because your files are on your local directory, your source URL will just be the location of your files on your local machine, and your destination will be the gs:// URI of your Terra workspace's GCS bucket. Please see [Google's docs on gsutil cp](https://cloud.google.com/storage/docs/gsutil/commands/cp) for more information.

### Situation 4: Local machine, using Terra's UI
Please see ["Option 1" in this article here](https://support.terra.bio/hc/en-us/articles/360024056512-Uploading-to-a-workspace-Google-bucket). Note that if there is an error, Terra will not automatically attempt to reupload, so you may only want to go this route with small files. For large files, consider the solution for Situation 3.

### Situation 5: Notebook VM using Terra's terminal
Please see documentation [here](https://hackmd.io/yXS65cyfTUSY8790vZzThw).

### Situation 6: Google Cloud Bucket, using gsutil
Similar to Situation 5, you can use Terra's terminal to move files around. This time, `gsutil cp` can be put to work moving files between a bucket you have downloader access to and your Terra workspace.

Note that if the GCS bucket you are transferring from is a requester pays bucket, you will need to provide your billing project ID in order to download the files. This billing project ID is the same one that you selected when making your workspace. If your billing project ID was terra-billing-test, then the resulting command would be:

`gsutil -u terra-billing-test cp gs://source-bucket gs://terra-workspace`

### Situation 7: Using the gs:// URI directly
In some cases, you might just be working with a few files. If so, you can skip the question of Source-to-Bucket entirely and just use the gs:// URI as your workflow input [as explained in Terra's pipeline documentation](https://support.terra.bio/hc/en-us/articles/360026521831-Configure-a-workflow-to-process-your-data#h_d8435f57-4713-40c5-b5af-150f1872057f). However, if you have enough files to make typing out the inputs tedious, it may be worth your time instead following the instructions for Situation 6.

### Situation 8: Google's from AWS
[Please see Google's documentation.](https://cloud.google.com/migrate/compute-engine/docs/4.8/how-to/migrate-aws-to-gcp/migrating-aws-vms)

## Bucket-to-Compute
Now that your data is on the system, let's discuss how to actually use it. The route you take depends on whether you want to run a WDL workflow on your files, or to run something within a Jupyter notebook on your files.

### Situation A: WDL workflow using gs:// URI
Using the Google Cloud Bucket address that your files now reside in, you can simply enter that address as an input into your workflow. [Terra's pipeline documentation](https://support.terra.bio/hc/en-us/articles/360026521831-Configure-a-workflow-to-process-your-data#h_d8435f57-4713-40c5-b5af-150f1872057f) explains how this is done. Remember, you can click on a file in Terra's data section to view it's full GCS address by just examining the `gsutil cp` command provided. You will likely have to scroll to read the full address.

![Image of a file in Terra's data section with its gs URI circled in green](https://raw.githubusercontent.com/DataBiosphere/BYOD-to-Terra/anvil/getting%20file%20address.png)

### Situation B: WDL workflow by creating data tables with the BYOD suite
Using the notebooks provided in this repo, you can create data tables that point to the location of your files. If you have used data tables in Terra before, you are likely familiar with their overall structure and their utility. As a simple example, let's say you uploaded 5 CRAMs to your workspace bucket. Using the notebook File Finder will give you a Terra data table with this structure:
![A table with five rows and two columns. The first column represents file names. The second column is for the location of those files, represented by a hyperlink.](https://raw.githubusercontent.com/DataBiosphere/BYOD-to-Terra/anvil/file%20finder%20cram%20table.png)

Although the second column looks like it just contains the file name on Terra's UI, cells in that column actually have the full gs URI of the file. If you click on the hyperlink in Terra's view of this data table, a popup showing where the gs URI of the file will appear.

When running a workflow on Terra, you have the option of using the contents of data table's column as an input in your workflow. This is the use case for which the BYDO suite was designed. If you need information on how to use data tables as workflow inputs, [please see Terra's documentation here](https://support.terra.bio/hc/en-us/articles/360026521831-Configure-a-workflow-to-process-your-data#h_602a754c-7e8d-4cc2-8a0b-6deef92b2f85).

### Situation C: Jupyter
Jupyter notebooks exist in a virtual machine on Terra, so depending on how your notebook is set up, you might need to transfer your files into the notebook VM. Please note that these instructions are focused on moving actual files into a notebook VM; if you wish to parse data tables instead, check out [terra-notebook-utils](https://github.com/DataBiosphere/terra-notebook-utils), which can parse dataframes by converting them to pandas dataframes.

Before moving your files to your notebook VM, consider if it is necessary. It might be more time-effective to adapt your notebook to use gsutil or [terra-noteook-utils](https://github.com/DataBiosphere/terra-notebook-utils) to query only the file names or some of their contents rather than downloading the entire file. For instance, [File Finder](https://github.com/DataBiosphere/BYOD-to-Terra/blob/master/File%20Finder.py) does not transfer the files it works upon from your workspace bucket, instead just querying Google for their file names.

Generally speaking, you will only need to take a separate step to move your files into your VM if you are using a Jupyter notebook that needs to parse an entire file.

If you are working with files behind DRS URIs, it is recommend to use [terra-noteook-utils](https://github.com/DataBiosphere/terra-notebook-utils)' functions of `drs.download()` and `drs.copy()` to transfer to the notebook VM/your workspace bucket respectively instead of the instructions below.