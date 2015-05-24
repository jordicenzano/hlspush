#!/usr/bin/env bash

if [ -z "$1" ]; then
    #Server to resume
    read -e -p "Enter server to resume (A or B) " SERVER_RESUME
else
    SERVER_RESUME=$1
fi

#Imported variables ------------
#Servers info
#REMOTE_IP_A
#REMOTE_IP_B
#-------------------------------

source ./cred/live_config.cfg

resume_remote()
{
REMOTE_IP=$1

ssh -i ./cred/newlivetestJordi.pem ec2-user@$REMOTE_IP << EOF
cd /home/ec2-user/hlspush
rm -f skip_upload
exit
EOF
}

# START SCRIPT *************************************

if [ "A" == "$SERVER_RESUME" ]; then
#Stop wowza processes in server A
resume_remote $REMOTE_IP_A
fi
if [ "B" == "$SERVER_RESUME" ]; then
resume_remote $REMOTE_IP_B
fi
echo "$(tput setaf 2)Resumed server $SERVER_RESUME$(tput sgr 0)"
