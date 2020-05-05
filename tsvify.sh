# This the precursor of the BYOD notebooks in this repository. It is meant to run on a local computer
# in order to generate a TSV file that can be uploaded as a Terra data table by predicting the gs://
# URI your files will have once uploaded. It may still have some use for those of you who, like me,
# can never remember how to use awk or sed but don't want to wait 2 minutes for an iPython notebook
# to spin up. If your data isn't on your local machine or you're on Windows this is of no use to you.

# Author: Ash O'Farrell
# Date created: March 24, 2020
# Date last modified: Time has no meaning in quarantine, but I think it's April 16th

# This was created on OSX Catalina. It should work on anything running BASH or ZSH.

# Make sure all of your files are in a folder. First of all, cd into that folder. 
# Then run this script. You will need to replace the gs:// line with your own workspace bucket's address.

GOOGLE_BUCKET=gs://fc-YOUR-BUCKET-HERE

# It turns out ls -I (--ignore) doesn't work if you have Catalina, nor does find -not, 
# so we have to hardcode the extensions to be included in order to exclude this shell script.
# If you're not using cram files, change the extension.

ls *.cram > contents.txt

# Append each line with the address of where the files live in your google bucket
awk -v GOOG=$GOOGLE_BUCKET '{print $0, "\t"GOOG$0}' contents.txt >> contentlocations.txt

# Because Mac OSX uses a different version of sed, awk is a better choice here
echo "entity:SOMETHING-WITHOUT-SPACES-OR-UNDERSCORES_id\tfilelocation" > temp.txt
cat temp.txt contentlocations.txt > final.tsv

# Clean up your directory
rm contents.txt contentlocations.txt temp.txt

# Go upload your TSV to Terra now. This six-liner will not upload it for you, because it's faster
# to use Terra's UI than to set up gsutil, especially if you're a Linux user that has an entirely
# different gsutil already installed, so you have to change your PATH, blah blah...