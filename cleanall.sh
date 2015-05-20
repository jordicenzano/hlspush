echo "Clean local"
./localclean.sh

echo "Deleting S3 hls-origin-jordi/A"
./s3clean.rb -f ./cred/.s3cfg -b "hls-origin-jordi" -p "A"

echo "Deleting S3 hls-origin-jordi/B"
./s3clean.rb -f ./cred/.s3cfg -b "hls-origin-jordi" -p "B"

echo "Deleting S3 hls-origin-jordi-bck/A"
./s3clean.rb -f ./cred/.s3cfg -b "hls-origin-jordi-bck" -p "A"

echo "Deleting S3 hls-origin-jordi-bck/B"
./s3clean.rb -f ./cred/.s3cfg -b "hls-origin-jordi-bck" -p "B"