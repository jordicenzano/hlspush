
if [ -z "$1" ]; then
    #Ask wowza IP
    read -e -p "Enter wowza server IP / URL? " url
else
    url=$1
fi

#Connect to server
ssh -i ./cred/WowzaNCA.pem ec2-user@$url
