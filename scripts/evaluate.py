import evaluation
import argparse

parser = argparse.ArgumentParser(description='Compare multiple trace files.')
parser.add_argument('yaml_config', type=str,
                            help='Location of YAML config file')
parser.add_argument('x_variable', type=str,
                            help='The variable that represents the x-axis on the figure')
parser.add_argument('output_fn', type=str,
                    help='Output filename')
parser.add_argument('trace_file', nargs='+',
                            help='Real-world trace file')

args = parser.parse_args()

evaluation.CompareTraces().analyze_trace(args.yaml_config, args.trace_file, args.x_variable, args.output_fn)

