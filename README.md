# expose
EXPOSE: Experimental Performance Evaluation of Stream Processing Systems Made Easy

To execute the experiments from the paper, please clone this repository in the home directory, e.g., ~/, on three devices. The first device serves as the coordinator. The second as the "intel_xeon" server, and the third is the "RPI".

Install Ansible on the coordinator machine, and add the following lines to /etc/ansible/hosts:
[intel_xeon]
<IP address of Intel Xeon server>

[raspberrypi_4]
<IP address of RPI>

Add the following line to /etc/ansible/ancible.cfg:
[ssh_connection]
ssh_args = -o ConnectTimeout=30 -o ConnectionAttempts=30

If `[ssh_connection]` and/or `ssh_args =` already exist, just add what's missing.

Make sure to place the public ssh key of the coordinator in the acknowledged_hosts file of both the Intel Xeon server and the RPI


Install the SPEs in SPEs-plus-wrappers/ by running `./init_all && ./build_all`

Install the software framework in Software-Framework by running `mvn install && ./add-to-local-maven-repo.sh`

By using the isolcpus kernel parameter, isolate one of the CPU cores in the Intel Xeon server and the RPI. We isolated the 19th CPU core of the Intel Xeon server and the 3rd CPU core of the RPI 4. Hyperthreading is also off in the Intel Xeon server.

Apache Flink specific: make sure to run Kafka on both servers, i.e., Zookeeper and the Kafka server. Run `taskset -cp <isolated core> <zookeeper PID> && taskset -cp <isolated core> <kafka-server PID>` on the servers to isolate Kafka as well.

Run experiments:
In the scripts folder, the RUN script contains the lines to run, in order to execute the experiments:
nohup ./vldb ~/expose/scripts/Experiments ../experiment-configurations/experiment-configuration-movsim-intel-xeon-vldb.yaml ../experiment-configurations/experiment-configuration-movsim-rpi4-vldb.yaml 19 3 &

nohup just ensures that all the output gets written to nohup.out and that it runs even though the user exits the terminal.

In order for the analysis scripts to run properly, several python libraries must be installed using pip. A recommended way of installing them is through a virtual environment, using virtualenv and virtualenvwrapper. Then run `pip install seaborn && pip install matplotlib && pip install numpy && pip install numpy_indexed && pip install pyyaml`.
