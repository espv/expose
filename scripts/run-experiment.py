import argparse
import os
import signal
import uuid

import yaml

import logging

import subprocess
logging.getLogger().setLevel(logging.CRITICAL)

parser = argparse.ArgumentParser(description='Get mean throughput and rsd over a set of runs')
parser.add_argument('yaml_config', type=str, help='Location of yaml config file')
parser.add_argument('coordinator_script', type=str, help='Location of coordinator script')
parser.add_argument('spe_script', type=str, help='Location of SPE instance script')

args = parser.parse_args()

all_child_processes = []

os.setpgrp()


def exit_handler(signum, frame):
    print('My application is ending!')
    for process in all_child_processes:
        group = os.getpgid(process.pid)
        print("Ending group", group)
        os.killpg(group, signal.SIGTERM)
        process.wait()
    os.killpg(os.getpgid(os.getpid()), signal.SIGKILL)


signal.signal(signal.SIGINT, exit_handler)
signal.signal(signal.SIGTERM, exit_handler)


class TraceAnalysis(object):
    @staticmethod
    def analyze_trace(yaml_config, coordinator_script, spe_script):
        print("Starting experiments")
        yaml_config = yaml.load(open(yaml_config))
        configuration = yaml_config["configuration"]
        SPEs = configuration["SPEs"]
        nodes = configuration["nodes"]
        coordinator = configuration["coordinator"]
        coordinator_ssh_user = coordinator["ssh-user"]
        coordinator_ssh_host = coordinator["ssh-host"]
        coordinator_port = str(coordinator["port"])
        coordinator_expose_path = coordinator.get("expose-path")
        experiment_configuration = configuration["experiment-configuration"]
        coordinator_env = os.environ.copy()
        coordinator_env["EXPOSE_PATH"] = coordinator_expose_path
        for spe in SPEs:
            experiments_to_run = spe["experiments-to-run"]
            for experiment_id in experiments_to_run:
                all_child_processes = []
                print("First start up the coordinator for", spe["name"], "experiments to run")
                script_call = "ssh -t -t " + coordinator_ssh_user + "@" + coordinator_ssh_host + " " + coordinator_script + " " + str(experiment_id) + " " + experiment_configuration + " " + coordinator_port
                print("script call:", script_call)
                coordinator_process = subprocess.Popen(script_call.split(), env=coordinator_env)
                all_child_processes.append(coordinator_process)
                print("PID of coordinator script:", coordinator_process.pid)
                print("")
                print("Then run all the nodes separately")
                spe_instances = []
                uuid_str = str(uuid.uuid4())
                for node in nodes:
                    spe_ssh_user = node.get("ssh-user")
                    spe_ssh_host = node.get("ssh-host")
                    isolated_cpu_cores = ",".join([str(node_id) for node_id in node.get("isolated-cpu-cores", [])])
                    node_ids = node.get("node-ids")
                    expose_path = node.get("expose-path")
                    SPE_env = os.environ.copy()
                    SPE_env["EXPOSE_PATH"] = expose_path
                    for node_id in node_ids:
                        script_call = "ssh -t -t " + spe_ssh_user + "@" + spe_ssh_host + " " + expose_path + "/scripts/kill_kafka"
                        spe_process = subprocess.Popen(script_call.split(), env=SPE_env)
                        spe_process.wait()
                        spe_instances.append(spe_process)
                        all_child_processes.append(spe_process)
                        script_call = "ssh -t -t " + spe_ssh_user + "@" + spe_ssh_host + " EXPOSE_PATH=" + expose_path + " " + spe_script + " " + spe["name"] + " " + str(node_id) + " " + spe_ssh_user + " " + spe_ssh_host + " \"" + isolated_cpu_cores + "\" " + coordinator_ssh_host + " " + coordinator_port + " " + str(experiment_id) + " " + uuid_str
                        print("script call:", script_call)
                        spe_process = subprocess.Popen(script_call.split(), env=SPE_env)
                        spe_instances.append(spe_process)
                        all_child_processes.append(spe_process)
                print()
                # Wait here until the coordinator has finished
                coordinator_process.wait()
                for spe_instance in spe_instances:
                    spe_instance.terminate()
                    spe_instance.wait()
                print("Coordinator has finished")


if __name__ == '__main__':
    TraceAnalysis.analyze_trace(args.yaml_config, args.coordinator_script, args.spe_script)
