# This the precursor of the BYOD notebooks in this repository. It is meant to run on a local computer
# in order to generate a TSV file that can be uploaded as a Terra data table by predicting the gs://
# URI your files will have once uploaded. It may still have some use for those of you who, like me,
# can never remember how to use awk or sed but don't want to wait 2 minutes for an iPython notebook
# to spin up. If your data isn't on your local machine or you're on Windows this is of no use to you.

# Author: Ash O'Farrell
# Date created: March 24, 2020
# Date last modified: May 19th, 2020

# This was created on OSX Catalina. It should work on anything running BASH or ZSH.

# Make sure all of your files are in a folder on your local machine. For the sake of simplicity
# this script is meant to be run in the same folder as the one where your files live. Make sure
# to edit GOOGLE_BUCKET, SOMETHING-WITHOUT-SPACES-OR-UNDERSCORES, and, if needed, the file
# extension you're indexing with ls.

# You will need to replace the gs:// line with your own workspace bucket's address.
GOOGLE_BUCKET=gs://fc-YOUR-BUCKET-HERE

# It turns out ls -I (--ignore) doesn't work if you have Catalina, nor does find -not, 
# so we have to hardcode the extensions to be included in order to exclude this shell script.

# If you're not using cram files, change the extension to whatever you're using.
ls *.cram > contents.txt

# Appends each line with the address of where the files live in your google bucket
# Because Mac OSX uses a different version of sed, awk is a better choice here
awk -v GOOG=$GOOGLE_BUCKET '{print $0, "\t"GOOG$0}' contents.txt >> contentlocations.txt

# You can (and should) replace SOMETHING-WITHOUT-SPACES-OR-UNDERSCORES, but make sure to
# leave the _id and everything after it in the string and the entity: at the beginning or
# else Terra will not accept the resulting TSV as valid. Whatever you put between entity: and
# _id will become the name of the table on Terra.
echo "entity:SOMETHING-WITHOUT-SPACES-OR-UNDERSCORES_id\tfilelocation" > temp.txt
cat temp.txt contentlocations.txt > final.tsv

# Clean up your directory
rm contents.txt contentlocations.txt temp.txt

# Go upload your TSV to Terra using their UI. You can do this in the data section by clicking the +
# icon next to the heading TABLES. There is a way to upload data tables from the command line, but
# it requires installing external packages so I recommend you just use Terra's UI.