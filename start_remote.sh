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

#Server A
ssh -t -t -i ./cred/newlivetestJordi.pem ec2-user@"$REMOTE_IP_A" << ENDSSH
    cd /home/ec2-user/hlspush

    #Kill previous processes related to the same stream
    ps aux | grep $STREAMNAME | grep hlsdownload.rb | grep -v grep | awk '{print \$2};' | xargs kill -9 >/dev/null 2>&1
    rm -f ./running/hlsdownload/$STREAMNAME
    ps aux | grep $STREAMNAME | grep hlslivehealth.rb | grep -v grep | awk '{print \$2};' | xargs kill -9 >/dev/null 2>&1
    rm -f ./running/hlshealth/$STREAMNAME

    Clean up section
    rm -r -f ./localtest/$STREAMNAME
    #rm -f skip_upload
    rm -f ./log/$STREAMNAME*

    #Run hls push process
    nohup ./hlsdownload.rb -d s3 -u "http://localhost:1935/"$APP_NAME"/ngrp:"$STREAMNAME"_all/playlist.m3u8" -l "./localtest/$STREAMNAME" -k "$S3KEY" -s "$S3SECRET" -r "$S3REGION" -b "$S3BUCKET" -m "$CACHECONTROL_CHUNKLISTS_S" -t "$CACHE_CONTROL_SEGMENTS_S" -y "$CACHECONTROL_MANIFESTS_S" -j "./skip_upload" -x "$S3BUCKET_BACKUP" -c "$ACCESS_SCHEMA" -p A -q B -f "$CLOUDFRONT_DIST" -v 1> ./log/"$STREAMNAME"_push.log 2>&1 < /dev/null &
    touch ./running/hlsdownload/$STREAMNAME

    #Run hls health process
    nohup ./hlslivehealth.rb -u ""$ACCESS_SCHEMA"://s3-"$S3REGION".amazonaws.com/"$S3BUCKET_BACKUP"/"$APP_NAME"/ngrp:"$STREAMNAME"_all/playlist.m3u8" -k "$S3KEY" -s "$S3SECRET" -r "$S3REGION" -f $SEGMENT_FAILURE_DETECTION_TRESHOLD > ./log/"$STREAMNAME"_health.log 2>&1 < /dev/null &
    touch ./running/hlshealth/$STREAMNAME

    exit
ENDSSH

#Kill previous processes related to the same stream
ps aux | grep $STREAMNAME | grep ffmpeg | grep $REMOTE_IP_A |grep -v grep | awk '{print $2};' | xargs kill -9 >/dev/null 2>&1

#Publish stream to A
echo "Running ffmpeg to wowza A, logs in ./log/ffmpegA"
ffmpeg -f lavfi -re -i testsrc=duration=36000:size=320x250:rate=25 -f lavfi -re -i "sine=frequency=1000:duration=36000:sample_rate=44100" -i ./pictures/p.png -filter_complex 'overlay=10:main_h-overlay_h-10' -pix_fmt yuv420p -c:v libx264 -b:v 500k -g 25 -profile:v baseline -preset veryfast -c:a libfaac -b:a 96k -f flv "rtmp://$WOWZA_PUBLISHING_USER:$WOWZA_PUBLISHING_PASS@$REMOTE_IP_A:1935/liveorigin/$STREAMNAME" > ./log/"$STREAMNAME"_ffmpegA.log 2> ./log/"$STREAMNAME"_ffmpegA_err.log &

#Server B
if [ -n "$REMOTE_IP_B" ]; then
    ssh -t -t -i ./cred/newlivetestJordi.pem ec2-user@"$REMOTE_IP_B" << ENDSSH
        cd /home/ec2-user/hlspush

        ps aux | grep $STREAMNAME | grep hlsdownload.rb | grep -v grep | awk '{print \$2};' | xargs kill -9 >/dev/null 2>&1
        rm -f ./running/hlsdownload/$STREAMNAME
        ps aux | grep $STREAMNAME | grep hlslivehealth.rb | grep -v grep | awk '{print \$2};' | xargs kill -9 >/dev/null 2>&1
        rm -f ./running/hlshealth/$STREAMNAME

        Clean up section
        rm -r -f ./localtest/$STREAMNAME
        #rm -f skip_upload
        rm -f ./log/$STREAMNAME*

        nohup ./hlsdownload.rb -d s3 -u "http://localhost:1935/"$APP_NAME"/ngrp:"$STREAMNAME"_all/playlist.m3u8" -l "./localtest/$STREAMNAME" -k "$S3KEY" -s "$S3SECRET" -r "$S3REGION" -b "$S3BUCKET_BACKUP" -m "$CACHECONTROL_CHUNKLISTS_S" -t "$CACHE_CONTROL_SEGMENTS_S" -y "$CACHECONTROL_MANIFESTS_S" -j "./skip_upload" -x "$S3BUCKET" -c "$ACCESS_SCHEMA" -p B -q A -f "$CLOUDFRONT_DIST" -v 1> ./log/"$STREAMNAME"_push.log 2>&1 < /dev/null &
        touch ./running/hlsdownload/$STREAMNAME

        nohup ./hlslivehealth.rb -u ""$ACCESS_SCHEMA"://s3-"$S3REGION".amazonaws.com/"$S3BUCKET"/"$APP_NAME"/ngrp:"$STREAMNAME"_all/playlist.m3u8" -k "$S3KEY" -s "$S3SECRET" -r "$S3REGION" -f $SEGMENT_FAILURE_DETECTION_TRESHOLD > ./log/"$STREAMNAME"_health.log 2>&1 < /dev/null &
        touch ./running/hlshealth/$STREAMNAME

        exit
    ENDSSH

    #Kill previous processes related to the same stream
    ps aux | grep $STREAMNAME | grep ffmpeg | grep $REMOTE_IP_A |grep -v grep | awk '{print $2};' | xargs kill -9 >/dev/null 2>&1

    #Publish stream to B
    echo "Running ffmpeg to wowza A, logs in ./log/ffmpegB"
    ffmpeg -f lavfi -re -i testsrc=duration=36000:size=320x250:rate=25 -f lavfi -re -i "sine=frequency=1000:duration=36000:sample_rate=44100" -i ./pictures/b.png -filter_complex 'overlay=10:main_h-overlay_h-10' -pix_fmt yuv420p -c:v libx264 -b:v 500k -g 25 -profile:v baseline -preset veryfast -c:a libfaac -b:a 96k -f flv "rtmp://$WOWZA_PUBLISHING_USER:$WOWZA_PUBLISHING_PASS@$REMOTE_IP_B:1935/liveorigin/$STREAMNAME" > ./log/"$STREAMNAME"_ffmpegB.log 2> ./log/"$STREAMNAME"_ffmpegB_err.log &

else
    echo "No failover server configured"
fi
echo "Streamname: $STREAMNAME"