echo "Starting accumulo"
docker run -it -h localhost -u hduser --name gaffer-accumulo --volumes-from gaffer -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix gaffer-docker/accumulo
