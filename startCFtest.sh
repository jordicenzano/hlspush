./cleanall.sh

if [ -z "$1" ]; then
    #Ask wowza IPA
    read -e -p "Enter wowzaA server IP / URL: " urlwA
else
    urlwA=$1
fi

if [ -z "$2" ]; then
    #Ask wowza IPB
    read -e -p "Enter wowzaB server IP / URL: " urlwB
else
    urlwB=$2
fi

if [ -z "$3" ]; then
    #Ask stream name
    read -e -p "Enter stream name (not repeat, CF problem): " streamName
else
    streamName=$3
fi

if [ -z "$4" ]; then
    #Ask stream name
    read -e -p "Enter wowza publishing user: " wuser
else
    wuser=$4
fi

if [ -z "$5" ]; then
    #Ask stream name
    read -e -p "Enter wowza publishing pass: " wpass
else
    wpass=$3
fi

./cleanall.sh

#Stream to A (depending on encoding settings the wowza used resources can change A LOT)
echo "Running ffmpeg to wowza A, logs in ./log/ffmpegA"
ffmpeg -f lavfi -re -i testsrc=duration=36000:size=320x250:rate=25 -f lavfi -re -i "sine=frequency=1000:duration=36000:sample_rate=44100" -i ~/Pictures/P.png -filter_complex 'overlay=10:main_h-overlay_h-10' -pix_fmt yuv420p -c:v libx264 -b:v 500k -g 25 -profile:v baseline -preset veryfast -c:a libfaac -b:a 96k -f flv "rtmp://$wuser:$wpass@$urlwA:1935/liveorigin/$streamName" > ./log/ffmpegA.log 2> ./log/ffmpegA_err.log &

#Stream to B
echo "Running ffmpeg to wowza B, logs in ./log/ffmpegB"
ffmpeg -f lavfi -re -i testsrc=duration=36000:size=320x250:rate=25 -f lavfi -re -i "sine=frequency=1000:duration=36000:sample_rate=44100" -i ~/Pictures/B.png -filter_complex 'overlay=10:main_h-overlay_h-10' -pix_fmt yuv420p -c:v libx264 -b:v 500k -g 25 -profile:v baseline -preset veryfast -c:a libfaac -b:a 96k -f flv "rtmp://$wuser:$wpass@$urlwB:1935/liveorigin/$streamName" > ./log/ffmpegB.log 2> ./log/ffmpegB_err.log &
