#!/usr/bin/env bash

if [ -z "$1" ]; then
    #Streamname
    read -e -p "Enter stream name " STREAMNAME
else
    STREAMNAME=$1
fi

#Imported variables ------------
#REMOTE_IP_A="54.215.81.31"
#REMOTE_IP_B=""

#S3 access info
#S3KEY
#S3SECRET
#S3REGION

#app info
#S3BUCKET
#S3BUCKET_BACKUP
#CACHECONTROL_MANIFESTS_S0
#CACHECONTROL_CHUNKLISTS_S
#CACHE_CONTROL_SEGMENTS_S
#ACCESS_SCHEMA
#CLOUDFRONT_DIST
#APP_NAME
#SEGMENT_FAILURE_DETECTION_TRESHOLD

#Publisher info
#WOWZA_PUBLISHING_USER
#WOWZA_PUBLISHING_PASS

#-----------------------------

source ./cred/live_config.cfg

start_remote()
{
REMOTE_IP=$1
LOCAL_S3_BUCKET_MAIN=$2
LOCAL_S3_BUCKET_BCK=$3
PREFIX_MAIN=$4
PREFIX_BCK=$5

ssh -i ./cred/newlivetestJordi.pem ec2-user@$REMOTE_IP << EOF
cd /home/ec2-user/hlspush

#Kill previous processes related to the same stream
ps aux | grep $STREAMNAME | grep hlsdownload.rb | grep -v grep | awk "{print \$2};" | xargs kill -9 >/dev/null 2>&1
rm -f ./running/hlsdownload/$STREAMNAME
ps aux | grep $STREAMNAME | grep hlslivehealth.rb | grep -v grep | awk "{print \$2};" | xargs kill -9 >/dev/null 2>&1
rm -f ./running/hlshealth/$STREAMNAME

#Clean up section
rm -r -f ./localtest/$STREAMNAME
rm -f skip_upload
rm -f ./log/$STREAMNAME*

#Run hls push process
nohup ./hlsdownload.rb -d s3 -u "http://localhost:1935/$APP_NAME/ngrp:"$STREAMNAME"_all/playlist.m3u8" -l "./localtest/$STREAMNAME" -k "$S3KEY" -s "$S3SECRET" -r "$S3REGION" -b "$LOCAL_S3_BUCKET_MAIN" -m $CACHECONTROL_CHUNKLISTS_S -t $CACHE_CONTROL_SEGMENTS_S -y $CACHECONTROL_MANIFESTS_S -j "./skip_upload" -x "$LOCAL_S3_BUCKET_BCK" -c "$ACCESS_SCHEMA" -p $PREFIX_MAIN -q $PREFIX_BCK -f "$CLOUDFRONT_DIST" -v 1 > ./log/"$STREAMNAME"_push.log 2>&1 < /dev/null &
touch ./running/hlsdownload/$STREAMNAME

#Run hls health process
nohup ./hlslivehealth.rb -u "$ACCESS_SCHEMA://s3-$S3REGION.amazonaws.com/$LOCAL_S3_BUCKET_BCK/$PREFIX_BCK/$APP_NAME/ngrp:"$STREAMNAME"_all/playlist.m3u8" -k "$S3KEY" -s "$S3SECRET" -r "$S3REGION" -t $SEGMENT_FAILURE_DETECTION_TRESHOLD > ./log/"$STREAMNAME"_health.log 2>&1 < /dev/null &
touch ./running/hlshealth/$STREAMNAME

exit
EOF
}

start_local()
{
REMOTE_IP=$1
LOG_SUFFIX=$2

#Kill local previous processes related to the same stream
ps aux | grep $STREAMNAME | grep ffmpeg | grep $REMOTE_IP |grep -v grep | awk '{print $2};' | xargs kill -9 >/dev/null 2>&1

#Publish stream to A
echo "$(tput setaf 2)Running ffmpeg to wowza $LOG_SUFFIX, logs in ./log/"$STREAMNAME"_ffmpeg$LOG_SUFFIX$(tput sgr 0)"
ffmpeg -f lavfi -re -i testsrc=duration=36000:size=320x250:rate=25 -f lavfi -re -i "sine=frequency=1000:duration=36000:sample_rate=44100" -i ./pictures/$LOG_SUFFIX.png -filter_complex 'overlay=10:main_h-overlay_h-10' -pix_fmt yuv420p -c:v libx264 -b:v 500k -g 25 -profile:v baseline -preset veryfast -c:a libfaac -b:a 96k -f flv "rtmp://$WOWZA_PUBLISHING_USER:$WOWZA_PUBLISHING_PASS@$REMOTE_IP:1935/liveorigin/$STREAMNAME" > ./log/"$STREAMNAME"_ffmpeg"$LOG_SUFFIX".log 2> ./log/"$STREAMNAME"_ffmpeg"$LOG_SUFFIX"_err.log &

#To publish a file (alpha stage)
VIDEO_FILE_TO_PUBLISH="/Users/jcenzano/Movies/bbb_sunflower_2160p_60fps_normal.mp4"
#ffmpeg -i "$VIDEO_FILE_TO_PUBLISH" -vcodec copy -c:a libfaac -b:a 96k -f flv "rtmp://$WOWZA_PUBLISHING_USER:$WOWZA_PUBLISHING_PASS@$REMOTE_IP:1935/liveorigin/$STREAMNAME" > ./log/"$STREAMNAME"_ffmpeg"$LOG_SUFFIX".log 2> ./log/"$STREAMNAME"_ffmpeg"$LOG_SUFFIX"_err.log &
}


# START SCRIPT *************************************

#Server wowza processes in server A
start_remote $REMOTE_IP_A $S3BUCKET $S3BUCKET_BACKUP "A" "B"

#Start publisher to A
start_local $REMOTE_IP_A "A"

#Server B
if [ -n "$REMOTE_IP_B" ]; then

    #Server wowza processes in server B
    start_remote $REMOTE_IP_B $S3BUCKET_BACKUP $S3BUCKET "B" "A"

    #Start publisher to B
    start_local $REMOTE_IP_B "B"
else
    echo "$(tput setaf 1)No failover server configured$(tput sgr 0)"
fi

echo "$(tput setaf 2)Finished OK. Streamname: $STREAMNAME$(tput sgr 0)"
