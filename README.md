# expose
EXPOSE: Experimental Performance Evaluation of Stream Processing Systems Made Easy

If you use this software, please cite the research paper found in https://link.springer.com/chapter/10.1007/978-3-030-84924-5_2.

Tested on Ubuntu 19.04, 
Required software packages:
- maven
- ant
- python
- python-pip
- Java version 8 (must be used when installing the Stream Processing Engines (SPEs))
  - Use update-alternatives to change java and javac to use Java 8
- Install these on Ubuntu with `apt-get install maven ant python python3-pip openjdk-8-jdk automake libtool cmake libboost1.67-all-dev  # Boost is for T-Rex, and you might need to use a different version`

Python pip packages required for analysis:
- numpy
- pyyaml
Use a virtual environment, and run `pip3 install numpy pyyaml`

To execute the experiments from the paper, please clone this repository in the home directory, e.g., ~/, on three devices. The first device serves as the coordinator, the data source node and the data sink node. The second as the powerful server, and the third is the resource-constrained server, both of which run the system under test (SUT).

The devices that participate in the same experiments must have each other's public SSH keys in the authorized keys file \href{https://www.ssh.com/ssh/authorized_keys/}.

# SPE installation
Install the software framework in Software-Framework by running `mvn install && ./add-to-local-maven-repo.sh`

Install the SPEs in SPEs-plus-wrappers/ by running `./init_all && ./build_all`
- T-Rex specific: T-Rex requires yaml-cpp, which you can install from https://github.com/jbeder/yaml-cpp.
- Apache Flink and Beam specific: edit the $SPE_FOLDER/kafka/config/server.properties and change the broker.id to a unique ID and insert the correct IP address in the advertised.listeners parameter.

By using the isolcpus kernel parameter, isolate one of the CPU cores in the Intel Xeon server and the RPI. We isolated the 19th CPU core of the Intel Xeon server and the 3rd CPU core of the RPI 4. Hyperthreading is also off in the Intel Xeon server.

Apache Flink and Beam specific: make sure to run Kafka on both servers, i.e., Zookeeper and the Kafka server. Run `taskset -acp <isolated core> <zookeeper PID> && taskset -acp <isolated core> <kafka-server PID>` on the servers to isolate Kafka as well.

# Setting up experiments
Edit the IP addresses of the coordinator and the hosts for the SPEs in $EXPOSE_PATH/execution-configurations/intel-xeon-nexmark-config.yaml and $EXPOSE_PATH/execution-configurations/rpi-nexmark-config.yaml.

# Running experiments
In the scripts folder, the RUN script contains the lines to run, in order to execute the experimets:
Intel xeon experiments:
nohup python3 -u run-experiment.py ~/expose/configurations/execution-configurations/intel-xeon-nexmark-config.yaml ~/expose/scripts/runCoordinator ~/expose/scripts/runSpe &disown

RPI experiments:
nohup python3 -u run-experiment.py ~/expose/configurations/execution-configurations/rpi-nexmark-config.yaml ~/expose/scripts/runCoordinator ~/expose/scripts/runSpe &disown

# Results
Results from the most recent set of experiments is stored in a file called trace_analysis.txt within the most recently created folder in $EXPOSE_PATH/scripts/run-experiments-output/. It is in the form of run-experiments-$TIMESTAMP-$UUID.
