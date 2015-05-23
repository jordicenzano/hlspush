echo "Clean local"
./localclean.sh

S3BUCKET="live-test-bcov"
S3BUCKET_BACKUP="live-test-bck-bcov"

echo "Deleting S3 $S3BUCKET/A"
./s3clean.rb -f ./cred/.s3cfgbcov -b "$S3BUCKET" -p "A"

echo "Deleting S3 hls-origin-jordi/B"
./s3clean.rb -f ./cred/.s3cfgbcov -b "$S3BUCKET" -p "B"

echo "Deleting S3 $S3BUCKET_BACKUP/A"
./s3clean.rb -f ./cred/.s3cfgbcov -b "$S3BUCKET_BACKUP" -p "A"

echo "Deleting S3 $S3BUCKET_BACKUP/B"
./s3clean.rb -f ./cred/.s3cfgbcov -b "$S3BUCKET_BACKUP" -p "B"