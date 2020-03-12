import errno
import yaml
import os
import re
import math

import numpy as np
import numpy_indexed as npi
import seaborn as sns
from matplotlib import pyplot as plt
import matplotlib.ticker as plticker

import logging
logging.getLogger().setLevel(logging.CRITICAL)


np.set_printoptions(edgeitems=30, linewidth=100000,
                    formatter=dict(float=lambda x: "%.3g" % x))

class CompareTraces(object):

    def mean_execution_time(self, recvd_times, fin_times):
        execution_times = np.array([f-r for f, r in zip(fin_times, recvd_times)])
        mean = np.mean(execution_times)
        return mean

    def std_execution_time(self, mean, recvd_times, fin_times):
        execution_times = np.array([f - r for f, r in zip(fin_times, recvd_times)])
        std = np.std(execution_times)
        return std

    def analyze_trace(self, yaml_config, trace_files, x_variable, output_fn):
        yaml_config = yaml.load(open(yaml_config))
        scaling_tracepoints = []

        for t in yaml_config.get("tracepoints"):
            if t.get("x_variable") == x_variable:
                scaling_tracepoints.append(t)

        scaling_events = {}
        milestone_events = {}
        # Read json config file and populate a dict that contains a mapping between tracepoints and categories
        # We only care about scaling and milestone events, and each event can only be categorized as one of them.
        for t in yaml_config.get("tracepoints"):
            if t.get("category").get("isScalingEvent"):
                scaling_events[t.get("id")] = True
            if t.get("category").get("isMilestoneEvent"):
                milestone_events[t.get("id")] = True

        all_graph_points = []
        all_execution_time_graph_points = []
        all_std_graph_points = []

        for trace_file in trace_files:
            x = 0

            throughput_threshold = 2000
            number_tuples_processed = 0
            first_throughput_timestamp = 0
            last_throughput_timestamp = 0
            graph_points = []
            execution_time_graph_points = []
            std_time_graph_points = []
            stddev = 0
            received_times = []
            finished_times = []
            offset = 0
            timestamp = 0
            received_start_experiment = False
            current_throughputs = []
            with open(trace_file) as infile:
                i = -1
                for l in infile:
                    i += 1
                    e = re.split('[\t\n]', l)
                    timestamp = int(e[1])
                    if offset == 0:
                        offset = timestamp
                    tracepointId = int(e[0])
                    tracepoint = None
                    for t in yaml_config.get("tracepoints"):
                        if t.get("id") == tracepointId:
                            tracepoint = t
                            break

                    if tracepoint["name"] == "Finished one set":
                        if number_tuples_processed > throughput_threshold:
                            time_diff = last_throughput_timestamp - first_throughput_timestamp
                            throughput = number_tuples_processed / (time_diff/1000000000)
                            current_throughputs.append(throughput)
                            print("Number tuples processed:", number_tuples_processed)
                            print("Throughput:", throughput, "tuples per second")
                            number_tuples_processed = 0
                    if scaling_events.get(tracepointId):
                        # The event is a scaling event and the x-axis will be affected
                        scaling_tp = tracepoint.get("name")
                        if len(current_throughputs) > 0:
                            throughput = sum(current_throughputs) / len(current_throughputs)
                            print("Throughput:", throughput, "tuples per second")
                            graph_points.append((x, first_throughput_timestamp, throughput))
                            mean = self.mean_execution_time(received_times, finished_times)
                            stddev = self.std_execution_time(mean, received_times, finished_times)
                            received_times = []
                            finished_times = []
                            execution_time_graph_points.append((x, first_throughput_timestamp, mean))
                            std_time_graph_points.append((x, first_throughput_timestamp, stddev))
                            number_tuples_processed = 0
                            current_throughputs = []

                        if scaling_tp == "Add Query":
                            if x_variable == "Number queries":
                                x += 1
                        elif scaling_tp == "Clear Queries":
                            if x_variable == "Number queries":
                                x = 0
                        elif scaling_tp == "Add Event":
                            x += 1
                        elif scaling_tp == "Clear Events":
                            x = 0
                        elif scaling_tp == "Increase number of subscribers":
                            if x_variable == "Number sinks":
                                x += 1
                        elif scaling_tp == "Increase number of publishers":
                            if x_variable == "Number sources":
                                x += 1
                        elif scaling_tp == "Increase number of publishers and subscribers":
                            if x_variable == "Number sources and sinks":
                                x += 1
                        else:
                            raise RuntimeError("Unidentified scaling tracepoint")
                        print(tracepointId, "is a scaling event, x is now", x)

                    else:
                        if x_variable == "timestamp":
                            x = int(e[1])
                            print("Time is", x)


                    if received_start_experiment is False:
                        if tracepoint.get("name") == "Start experiment":
                            received_start_experiment = True
                        continue

                    # Always starting at x>0
                    if x == 0:
                        continue

                    if milestone_events.get(tracepointId):
                        if tracepoint["name"] == "Receive Event":
                            received_times.append(timestamp)
                            pass
                        elif tracepoint["name"] == "Passed Constraints":
                            pass
                        elif tracepoint["name"] == "Created Complex Event":
                            pass
                        elif tracepoint["name"] == "Finished Processing Event":
                            finished_times.append(timestamp)
                            if number_tuples_processed == 0:
                                first_throughput_timestamp = timestamp
                            last_throughput_timestamp = timestamp
                            number_tuples_processed += 1

                    if len(current_throughputs) > 0:
                        throughput = sum(current_throughputs) / len(current_throughputs)
                        print("Throughput:", throughput, "tuples per second")
                        graph_points.append((x, first_throughput_timestamp, throughput))
                        mean = self.mean_execution_time(received_times, finished_times)
                        stddev = self.std_execution_time(mean, received_times, finished_times)
                        received_times = []
                        finished_times = []
                        execution_time_graph_points.append((x, first_throughput_timestamp, mean))
                        std_time_graph_points.append((x, first_throughput_timestamp, stddev))
                        number_tuples_processed = 0
                        current_throughputs = []

                all_graph_points.append(graph_points)
                all_execution_time_graph_points.append(execution_time_graph_points)
                all_std_graph_points.append(std_time_graph_points)

        SMALL_SIZE = 14
        MEDIUM_SIZE = 16
        BIGGER_SIZE = 18
        my_dpi=100
        plt.rc('font', size=MEDIUM_SIZE)  # controls default text sizes
        plt.rc('axes', titlesize=MEDIUM_SIZE)  # fontsize of the axes title
        plt.rc('axes', labelsize=MEDIUM_SIZE)  # fontsize of the x and y labels
        plt.rc('xtick', labelsize=MEDIUM_SIZE)  # fontsize of the tick labels
        plt.rc('ytick', labelsize=MEDIUM_SIZE)  # fontsize of the tick labels
        plt.rc('legend', fontsize=SMALL_SIZE)  # legend fontsize
        plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title
        fig, ax = plt.subplots()
        fig.set_size_inches(4.5, 3.5, forward=True)
        ax.set_ylabel(r"Tuples per second")
        ax.set_xlabel(x_variable)
        plt.title("Throughput")
        for (trace_file, graph_points) in zip(trace_files, all_graph_points):
            ax.plot([a[0] for a in graph_points], [a[2] for a in graph_points], label=trace_file.split("/")[-1].split("ExperimentFramework")[0])

        ax.legend()
        fig.savefig(output_fn+'_throughput.png', dpi=my_dpi, bbox_inches='tight')

        fig, ax = plt.subplots()
        fig.set_size_inches(4.5, 3.5, forward=True)

        ax.set_ylabel(u"Time (µs)")
        ax.set_xlabel(x_variable)
        plt.title("Average execution time")
        for (trace_file, (std_graph_points, execution_time_graph_points)) in zip(trace_files, zip(all_std_graph_points, all_execution_time_graph_points)):
            xaxis = [a[0] for a in execution_time_graph_points]
            yaxis = [a[2]/1000 for a in execution_time_graph_points]
            ax.plot(xaxis, yaxis, label=trace_file.split("/")[-1].split("ExperimentFramework")[0])

        ax.legend()
        fig.savefig(output_fn+'_avg-execution-time.png', bbox_inches='tight')

