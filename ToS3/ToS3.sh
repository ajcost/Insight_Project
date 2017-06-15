#!/bin/bash
# This script pulls all submission and comment Json data from files.pushshift.io.
# Then the script expands all files using bzip2
# Finally the files are uploaded to S3 to the specific bucket s3://ac-reddit-data/Raw/

wget -r -nH -nd -np -R index.html* http://files.pushshift.io/reddit/comments/
wait

wget -r -nH -nd -np -R index.html* http://files.pushshift.io/reddit/submissions/
wait

bzip2 -d *.bz2
wait

for filename in ./*; do
  aws s3 cp "$filename" s3://ac-reddit-data/Raw/
  wait
done