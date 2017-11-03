import argparse
import numpy as np
from subprocess import call

def command_get_csv(db_name, out_path, papers):
    return "python get_csv.py --db_name %s --out_path %s --num_paper  %s"%(db_name, out_path, ' '.join(map(str, papers)))

def command_experiment(input, output_path, num_fragment):
    return "python experiment.py --input %s --output_path %s --num_fragment %s"%(input, output_path, num_fragment)

def command_gen_graph():
    return ""

def gen_fold(num_paper, n_fold):
    list = np.arange(num_paper) # generate array ex: 0,1,2,3,4,5,6,...,10
    p.random.shuffle(list) # shuffle array ex: 2,8,6,7,10,9,1,3,5,4
    return np.split(list,n_fold) # split the array ex [1,5,7],[4,3,2],[8,9,6],[0]

def cross:
    folds = gen_fold(1000, 5)
    for folds in folds:

def parser_args():
    parser = argparse.ArgumentParser(description='Get a stylometry synthetic data.')
    parser.add_argument('--fragment_size', type=int, help='number of chunks in a fragment')
    parser.add_argument('--num_fragment', type=int, help='number of fragment in a section')
    parser.add_argument('--chunk_size', type=int, help='number of chunk in a fragment')
    parser.add_argument('--num_chunk', type=int, help='number of chunk in a fragment')
    parser.add_argument('--num_chunk_per_section', type=int, help='number of chunk in a section')

    parser.add_argument('--num_paper', type=int, help='number of paper')
    parser.add_argument('--offset', type=int, help="number of chunks between n and n+1 fragment")
    parser.add_argument('--db_name', type=str, nargs='*', help="database name that want to get")
    parser.add_argument('--out_path', type=str, help="output path", default='.')
    return parser.parse_args()


if __name__ == "main":
    arg = parser_args()