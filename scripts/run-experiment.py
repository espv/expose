import argparse
import logging
import os
import sys
import re
import signal
import subprocess
import time
import uuid

import numpy as np
import yaml

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

    def get_std(self, numbers):
        n = np.array(numbers)
        return np.std(n)

    def analyze_trace(self, yaml_config, trace_file):
        yaml_config = yaml.load(open(yaml_config))
        milestone_events = {}
        first_received = 0.0
        last_received = 0.0
        number_tuples_received = 0.0
        throughputs = []
        run = 0
        received_start_experiment = False
        first_throughput = None

        with open(trace_file) as infile:
            print("Parsing through", trace_file)
            i = -1
            for l in infile:
                i += 1
                e = re.split('[\t\n]', l)
                print("Length of e:", len(e))
                if len(e) < 2:
                    break
                timestamp = int(e[1])
                tracepointId = int(e[0])
                tracepoint = None
                for t in yaml_config.get("tracepoints"):
                    if t.get("id") == tracepointId:
                        tracepoint = t
                        break

                if received_start_experiment is False:
                    if tracepoint.get("name") == "Start experiment":
                        received_start_experiment = True

                if tracepoint["name"] == "Finished one set":
                    throughput = number_tuples_received / ((last_received-first_received) / 1000000000.0)
                    if first_throughput is None:
                        first_throughput = throughput
                    if received_start_experiment is True:
                        throughputs.append(throughput)
                        run += 1
                    number_tuples_received = 0.0
                    last_received = 0.0
                    first_received = 0.0
                    pass

                if tracepoint["name"] == "Receive Event":
                    number_tuples_received += 1
                    if first_received == 0:
                        first_received = timestamp
                    last_received = timestamp
                elif tracepoint["name"] == "Passed Constraints":
                    pass
                elif tracepoint["name"] == "Created Complex Event":
                    pass
                elif tracepoint["name"] == "Finished Processing Event":
                    last_received = timestamp
                    pass

        if first_throughput is None:
            return
        print("Statistics for trace:", trace_file)
        print("Iteration 1 of warmup:", first_throughput, "tuples per second")
        for i, throughput in enumerate(throughputs):
            print("Run", i+1, ":", throughput, "tuples per second")
        mean = np.mean(np.array(throughputs))
        print("Average throughput over", run, "runs:", mean)
        print("The average throughput is", int((mean/first_throughput)*100), "% higher than the throughput for the first iteration")
        sd = np.std(np.array(throughputs), ddof=1)
        print("Sample SD over", run, "runs:", sd)
        rsd = 100.0 * (sd / mean)
        print("RSD% over", run, "runs:", rsd)


class RunExperiments(object):
    @staticmethod
    def get_unique_id():
        ret = str(time.time() * 1000)[:10] + "-" + str(uuid.uuid4())[:4]
        return ret

    @staticmethod
    def run_experiments(yaml_config, coordinator_script, spe_script):
        print("Starting experiments")
        yaml_config = yaml.load(open(yaml_config))
        configuration = yaml_config["configuration"]
        SPEs = configuration["SPEs"]
        hosts = configuration["hosts"]
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
                run_id = RunExperiments.get_unique_id()
                for host in hosts:
                    spe_ssh_user = host.get("ssh-user")
                    spe_ssh_host = host.get("ssh-host")
                    isolated_cpu_cores = ",".join([str(cpu_core) for cpu_core in host.get("isolated-cpu-cores", [])])
                    node_ids = host.get("node-ids")
                    expose_path = host.get("expose-path")
                    SPE_env = os.environ.copy()
                    SPE_env["EXPOSE_PATH"] = expose_path
                    for node_id in node_ids:
                        script_call = "ssh -t -t " + spe_ssh_user + "@" + spe_ssh_host + " " + expose_path + "/scripts/kill_kafka"
                        spe_process = subprocess.Popen(script_call.split(), env=SPE_env)
                        spe_process.wait()
                        spe_instances.append(spe_process)
                        all_child_processes.append(spe_process)
                        script_call = "ssh -t -t " + spe_ssh_user + "@" + spe_ssh_host + " EXPOSE_PATH=" + expose_path + " " + spe_script + " " + spe["name"] + " " + str(node_id) + " " + " \"" + isolated_cpu_cores + "\" " + coordinator_ssh_host + " " + coordinator_port + " " + str(experiment_id) + " " + run_id
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

        path = "run-experiments-output"
        if not os.path.exists(path):
            os.mkdir(path)
        run_experiments_output_folder = path + "/run-experiments-" + RunExperiments.get_unique_id()
        os.mkdir(run_experiments_output_folder)

        for host in hosts:
            log_name = "root_log_" + RunExperiments.get_unique_id()
            spe_ssh_user = host.get("ssh-user")
            spe_ssh_host = host.get("ssh-host")
            expose_path = host.get("expose-path")
            log_folder_path = expose_path + "/scripts/Experiments/archive/" + log_name

            ssh_call = "ssh " + spe_ssh_user + "@" + spe_ssh_host

            # Create archive folder if it doesn't exist
            cmd = "mkdir -p " + expose_path + "/scripts/Experiments/archive"
            script_call = ssh_call + " " + cmd
            subprocess.Popen(script_call.split()).wait()

            # Move log folder to archive
            cmd = "mv " + expose_path + "/scripts/Experiments/log " + log_folder_path
            script_call = ssh_call + " " + cmd
            subprocess.Popen(script_call.split()).wait()
            zip_file_name = log_name + ".tar.gz"
            zip_file_path = log_folder_path + ".tar.gz"

            # Create zip file
            cmd = "tar -C  " + expose_path + "/scripts/Experiments/archive -cf " + zip_file_path + " " + log_name
            script_call = ssh_call + " " + cmd
            subprocess.Popen(script_call.split()).wait()

            # Transfer zip file
            local_log_folder = run_experiments_output_folder + "/" + log_name
            script_call = "scp " + spe_ssh_user + "@" + spe_ssh_host + ":" + zip_file_path + " " + local_log_folder + ".tar.gz"
            print(script_call)
            subprocess.Popen(script_call.split()).wait()
            print("ls zip:")
            subprocess.Popen(["ls", local_log_folder + ".tar.gz"]).wait()
            script_call = "tar -C " + run_experiments_output_folder + " -xf " + local_log_folder + ".tar.gz"
            print("Extracting zip with", script_call)
            subprocess.Popen(script_call.split()).wait()

            # Remove zip file
            os.remove(local_log_folder + ".tar.gz")

            print("Performing trace analysis")
            # Perform trace analysis on the traces and write all traces from a node to the same file
            # analyze_trace will print to std out, and we redirect it to a file in the log_folder_path
            print("Redirecting standard output to", local_log_folder + "/trace_analysis.txt")
            sys.stdout = open(local_log_folder + "/trace_analysis.txt", 'w+')
            for path, subdirs, files in os.walk(local_log_folder):
                for name in files:
                    if name.endswith(".trace"):
                        TraceAnalysis().analyze_trace(experiment_configuration, path + "/" + name)
            sys.stdout = sys.__stdout__


if __name__ == '__main__':
    RunExperiments.run_experiments(args.yaml_config, args.coordinator_script, args.spe_script)
