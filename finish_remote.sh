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

stop_remote()
{
REMOTE_IP=$1

ssh -i ./cred/newlivetestJordi.pem ec2-user@$REMOTE_IP << EOF
cd /home/ec2-user/hlspush

#Kill processes related to this stream
ps aux | grep $STREAMNAME | grep hlsdownload.rb | grep -v grep | awk "{print \$2};" | xargs kill -9 >/dev/null 2>&1
rm -f ./running/hlsdownload/$STREAMNAME
ps aux | grep $STREAMNAME | grep hlslivehealth.rb | grep -v grep | awk "{print \$2};" | xargs kill -9 >/dev/null 2>&1
rm -f ./running/hlshealth/$STREAMNAME

#Clean up section
rm -r -f ./localtest/$STREAMNAME
#rm -f skip_upload
rm -f ./log/$STREAMNAME*

exit
EOF
}

stop_local()
{
REMOTE_IP=$1

#Kill local processes related to this stream
ps aux | grep $STREAMNAME | grep ffmpeg | grep $REMOTE_IP |grep -v grep | awk "{print $2};" | xargs kill -9 >/dev/null 2>&1
}

# START SCRIPT *************************************

#Stop wowza processes in server A
stop_remote $REMOTE_IP_A

#Srop publisher to A
stop_local $REMOTE_IP_A

#Server B
if [ -n "$REMOTE_IP_B" ]; then

    #Stop wowza processes in server B
    stop_remote $REMOTE_IP_B

    #Stop publisher to B
    stop_local $REMOTE_IP_B

else
    echo "$(tput setaf 1)No failover server configured"
fi

echo "$(tput setaf 2)Finished OK. Streamname: $STREAMNAME"
