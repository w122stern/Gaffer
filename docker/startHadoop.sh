echo "Starting hadoop"
docker run -it -h localhost -u hduser --name gaffer-hadoop -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix gaffer-docker/hadoop
echo "Hadoop started"
