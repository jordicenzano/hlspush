#!/usr/bin/env bash

if [ -z "$1" ]; then
    #Streamname
    read -e -p "Enter stream name " STREAMNAME
else
    STREAMNAME=$1
fi

#Imported variables ------------
#Servers info
#REMOTE_IP_A
#REMOTE_IP_B
#-------------------------------

source ./cred/live_config.cfg

#Server A
ssh -t -t -i ./cred/newlivetestJordi.pem ec2-user@"$REMOTE_IP_A" << ENDSSH
    cd /home/ec2-user/hlspush

    ps aux | grep $STREAMNAME | grep hlsdownload.rb | grep -v grep | awk '{print \$2};' | xargs kill -9 >/dev/null 2>&1
    rm -f ./running/hlsdownload/$STREAMNAME
    ps aux | grep $STREAMNAME | grep hlslivehealth.rb | grep -v grep | awk '{print \$2};' | xargs kill -9 >/dev/null 2>&1
    rm -f ./running/hlshealth/$STREAMNAME

    #Clean up section
    rm -r -f ./localtest/$STREAMNAME
    #rm -f skip_upload
    rm -f ./log/$STREAMNAME*

    exit
ENDSSH

#Kill previous processes related to the same stream
ps aux | grep $STREAMNAME | grep ffmpeg | grep $REMOTE_IP_A |grep -v grep | awk '{print $2};' | xargs kill -9 >/dev/null 2>&1

#Server B
if [ -n "$REMOTE_IP_B" ]; then
    ssh -t -t -i ./cred/newlivetestJordi.pem ec2-user@"$REMOTE_IP_B" << ENDSSH1
        cd /home/ec2-user/hlspush

        ps aux | grep $STREAMNAME | grep hlsdownload.rb | grep -v grep | awk '{print \$2};' | xargs kill -9 >/dev/null 2>&1
        rm -f ./running/hlsdownload/$STREAMNAME
        ps aux | grep $STREAMNAME | grep hlslivehealth.rb | grep -v grep | awk '{print \$2};' | xargs kill -9 >/dev/null 2>&1
        rm -f ./running/hlshealth/$STREAMNAME

        #Clean up section
        rm -r -f ./localtest/$STREAMNAME
        #rm -f skip_upload
        rm -f ./log/$STREAMNAME*

        exit
    ENDSSH1

    #Kill previous processes related to the same stream
    ps aux | grep $STREAMNAME | grep ffmpeg | grep $REMOTE_IP_B |grep -v grep | awk '{print $2};' | xargs kill -9 >/dev/null 2>&1

else
    echo "No failover server configured"
fi
echo "Streamname: $STREAMNAME killed"