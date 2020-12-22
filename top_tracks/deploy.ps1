rm -r ./libs
pip3 install -r requirements.txt -t ./libs
rm -r libs/*dist-info

rm *zip
Compress-Archive -Path * -DestinationPath top_tracks.zip
#zip top_tracks.zip

aws s3 rm s3://jongs-lambda/top_tracks.zip
aws s3 cp ./top_tracks.zip s3://jongs-lambda/top_tracks.zip
aws lambda update-function-code --function-name top-tracks --s3-bucket jongs-lambda --s3-key top_tracks.zip
