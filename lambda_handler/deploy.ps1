rm -r ./libs
pip3 install -r requirements.txt -t ./libs
rm -r libs/*dist-info

rm *zip
Compress-Archive -Path * -DestinationPath spotify.zip


aws s3 rm s3://jongs-lambda/spotify.zip
aws s3 cp ./spotify.zip s3://jongs-lambda/spotify.zip
aws lambda update-function-code --function-name jongs-spotify --s3-bucket jongs-lambda --s3-key spotify.zip
