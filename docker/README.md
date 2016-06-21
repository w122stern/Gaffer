# Gaffer Docker

## Approach used to Dockerise Gaffer

Gaffer is packaged using docker by adding dependencies using a hierarchy of docker images. This reduces the size of the docker image and adheres to guides lines on the Docker GitHub for creating Docker images.

A base image  is created initially consisting of a base OS with the additional packages required for the installation of Gaffer, Hadoop and Accumulo.

The gaffer is based on the base image and is used to create the Gaffer container

The hadoop image is based on the base image and is used to create a hadoop container.

The accumulo image is based on the hadoop image and is used to create an Accumulo container.

The Accumulo container accesses the Gaffer container

## Structure

| Image Name                      | Usage                                                                       |
|---------------------------------|-----------------------------------------------------------------------------|
| gaffer-docker/accumulo          |( Dockerfile is used to create an Accumulo (1.6.4), zookeeper (3.3.5) on
|                                 |Apache Hadoop 2.6.0 image. Apache Hadoop 2.6.0 is inherited from
|                                 | the gaffer-docker/hadoop image . The image is used to create an
|                                 |Accumulo docker container ).
| gaffer-docker/hadoop            |( Dockerfile is used to create the an Apache Hadoop 2.6.0 image. The linux
|                                 |dependencies packages required for the installation of Apache Hadoop 2.6.0
|                                 |and firefox are inherited from the gaffer-docker/base image ). The
|                                 |image is used to create a Hadoop docker container ).
| gaffer-docker/gaffer            |( Dockerfile is used to create an image containing
|                                 |the GAFFER jar files and example jar files used to test gaffer.
|                                 |The linux dependencies required for the generation of the jar
|                                 |files from the Gaffer source code is inherited from the
|                                 |gaffer-docker/base image.  The image is used to create the gaffer
|                                 |data volume container )
| gaffer-docker/base              |( Image is used to create a container with the base centos6.7 image, obtained
|                                 |from saved copy of official Docker centos 6 image )


## Pre-requisites

1. The installation of Docker on the host - access to the internet is required for this step

2. Docker requires a 64-bit installation regardless of the CentOS version. The kernel must be 3.10 at minimum,
   which CentOS 7 runs. Installation needs to be run as root. See [docker install docs](https://docs.docker.com/engine/installation)

3. X11 package on host machine


## Quick start guide
1. Ensure docker is running, as root run:

```
service docker start
```

2. Change to docker folder:

```
cd Gaffer/docker
```

3. Run setup script as root (This sets swappiness to 0 and disables IPV6 on the Docker host):

```
./setup.sh
```

4. Run buildAndRun script as your user (note this will log you into the accumulo container):

`
./buildAndRun.sh
`

5. Start accumulo by running:

```
~/start-accumulo.sh
```

6. To disconnect from accumulo container:

```
Ctrl-p  Ctrl-q
```

7. To reattach to accumulo container run the attach script as your user:

```
./attach-accumulo.sh
```

8. To restart accumulo, run the stop and start scripts as your user:

```
./stop.sh
./start.sh
```


## Useful notes:

1. Hadoop

- To test run the command below:

```
docker run -it -h localhost -u hduser --name gaffer-hadoop -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix gaffer-docker/hadoop
```

- Login info:

  | Item                            | Description                                | Value  |
  |---------------------------------|--------------------------------------------|--------|
  | Hadoop  Process User            | User used to run hadoop processes          | hduser |
  | Hadoop Process User Password    | Password                                   | admin  |

   
2. Accumulo
   
- Login info:

  | Item                            | Description                                | Value  |
  |---------------------------------|--------------------------------------------|--------|
  | Accumulo Process User           | User used to run accumulo/hadoop processes | hduser |
  | Accumulo Process User Password  | Password                                   | admin  |
  | Root                            | Privileged user                            | admin  |
  | Accumulo Instance Name          | Name of accumulo instance                  | Gaffer |
  | Accumulo Instance User          | Name of user used to initialise instance   | root   |
  | Accumulo Instance User password | Password for accumulo instance user        | admin  |
  
  
3. Test Gaffer on Accumulo

- Whilst logged into the accumulo container, run the following script to copy the gaffer example jar files to the accumulo lib directory
  
```
~/gaffer2-setup.sh
```

- Create user and table with relevant authorizations to run the film LoadAndQuery example

  ```$ /opt/accumulo/bin/accumulo shell -u root -p admin```

  ```$ root@Gaffer> createuser user01```

  ```$ Enter new password for 'user01': password```

  ```$ Please confirm new password for 'user01': password```

  ```$ root@Gaffer> grant -s System.CREATE_TABLE -u user01```

  ```$ root@Gaffer> user user01```

  ```$ Enter password for user user01: password```

  ```$ user01@Gaffer> createtable table1```

  ```$ user01@Gaffer table1> user root```

  ```$ Enter password for user root: admin```

  ```$ root@Gaffer table1> setauths -s U,PG,_12A,_15,_18 -u user01```

  ```$ root@Gaffer table1> exit```

- Run LoadAndQuery example

  ```$ /opt/accumulo/bin/accumulo gaffer.example.films.analytic.LoadAndQuery```
  
- Output similar to below should be displayed:

```
> [hduser@localhost : /opt/accumulo/bin]$ ./accumulo gaffer.example.films.analytic.LoadAndQuery
> 2016-05-18 12:20:07,756 [client.ClientConfiguration] WARN : Found no client.conf in default paths. Using default client configuration values.
> 2016-05-18 12:20:08,994 [analytic.LoadAndQuery] INFO : Results from query:
> Entity{vertex=filmA, group='review', properties={starRating=<java.lang.Float>2.5, count=<java.lang.Integer>2, userId=<java.lang.String>user01,user03, rating=<java.lang.Long>100}}
```