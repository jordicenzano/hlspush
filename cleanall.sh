echo "Clean local"
./localclean.sh

echo "Deleting S3 hls-origin-jordi/Aliveorigin"
./s3clean.rb -f ./cred/.s3cfg -b "hls-origin-jordi" -p "Aliveorigin"

echo "Deleting S3 hls-origin-jordi/Bliveorigin"
./s3clean.rb -f ./cred/.s3cfg -b "hls-origin-jordi" -p "Bliveorigin"

echo "Deleting S3 hls-origin-jordi/liveorigin"
./s3clean.rb -f ./cred/.s3cfg -b "hls-origin-jordi" -p "liveorigin"

echo "Deleting S3 hls-origin-jordi-bck/liveorigin"
./s3clean.rb -f ./cred/.s3cfg -b "hls-origin-jordi-bck" -p "liveorigin"
