#!/usr/bin/env bash

if [ -z "$1" ]; then
    #Server to pause
    read -e -p "Enter server to pause (A or B) " SERVER_PAUSE
else
    SERVER_PAUSE=$1
fi

#Imported variables ------------
#Servers info
#REMOTE_IP_A
#REMOTE_IP_B
#-------------------------------

source ./cred/live_config.cfg

pause_remote()
{
REMOTE_IP=$1

ssh -i ./cred/newlivetestJordi.pem ec2-user@$REMOTE_IP << EOF
cd /home/ec2-user/hlspush
touch skip_upload
exit
EOF
}

# START SCRIPT *************************************

if [ "A" == "$SERVER_PAUSE" ]; then
#Stop wowza processes in server A
pause_remote $REMOTE_IP_A
fi
if [ "B" == "$SERVER_PAUSE" ]; then
pause_remote $REMOTE_IP_B
fi
echo "$(tput setaf 2)Paused server $SERVER_PAUSE$(tput sgr 0)"
