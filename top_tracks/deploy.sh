#!/bin/bash

rm -rf ./libs
pip3 install -r requirements.txt -t ./libs

rm *.zip
zip top_tracks.zip -r *

aws s3 rm s3://top-tracks-lambda/top_tracks.zip
aws s3 cp ./top_tracks.zip s3://top-tracks-lambda/top_tracks.zip
aws lambda update-function-code --function-name top-tracks --s3-bucket top-tracks-lambda --s3-key top_tracks.zip
