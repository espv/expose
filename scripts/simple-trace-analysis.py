import argparse
import yaml
import re
import numpy as np

import logging
logging.getLogger().setLevel(logging.CRITICAL)

parser = argparse.ArgumentParser(description='Get mean throughput and rsd over a set of runs')
parser.add_argument('yaml_config', type=str,
                    help='Location of yaml config file')
parser.add_argument('trace_file', type=str,
                    help='Real-world trace file')

args = parser.parse_args()

type_dict = {
    "integer": int,
    "float": float,
    "string": str
}


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

        with open(trace_file) as infile:
            i = -1
            for l in infile:
                i += 1
                e = re.split('[\t\n]', l)
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
                    continue

                if received_start_experiment is not False and tracepoint["name"] == "Finished one set":
                    throughput = number_tuples_received / ((last_received-first_received) / 1000000000.0)
                    throughputs.append(throughput)
                    number_tuples_received = 0.0
                    last_received = 0.0
                    first_received = 0.0
                    run += 1
                    pass

                if tracepoint["name"] == "Receive Event":
                    number_tuples_received += 1
                    if first_received == 0:
                        first_received = timestamp
                elif tracepoint["name"] == "Passed Constraints":
                    pass
                elif tracepoint["name"] == "Created Complex Event":
                    pass
                elif tracepoint["name"] == "Finished Processing Event":
                    last_received = timestamp

        for i, throughput in enumerate(throughputs):
            print("Run 1:", throughput, "tuples per second")
        mean = np.mean(np.array(throughputs))
        print("Average throughput over", run, "runs:", mean)
        sd = np.std(np.array(throughputs), ddof=1)
        print("Sample SD over", run, "runs:", sd)
        rsd = 100.0 * (sd / mean)
        print("RSD% over", run, "runs:", rsd)


if __name__ == '__main__':
    TraceAnalysis().analyze_trace(args.yaml_config, args.trace_file)
