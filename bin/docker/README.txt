To create the runtime dockers images:
$ cd /presto/presto-native-execution/

First build the dependency image presto/prestissimo-dependency:centos9:

$ docker compose build centos-native-dependency

Then build the runtime image presto/prestissimo-runtime:centos9 from that:

$ GPU=ON docker compose build --build-arg NUM_THREADS=256 --build-arg CUDA_ARCHITECTURES=80 centos-native-runtime
You may need to delete older images, e,g.

$ docker rmi quay.io/centos/centos:stream9

Finally, give it a meaningful name with the velox commit such that it doesn't get overwritten
$ docker tag presto/prestissimo-runtime:centos9 presto/my-image:mytag
