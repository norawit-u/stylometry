import os
import argparse
import subprocess
import numpy as np
from subprocess import call


def command_get_csv(db_name, out_path, papers, note):
    return "python get_csv.py --db_name %s --out_path %s --papers  %s --note %s" % (
        db_name, out_path, ' '.join(map(str, papers)), note)


def command_experiment(input, output_path, num_fragment):
    return "python experiment_old.py --input %s --output_path %s --num_fragment %s" % (input, output_path, int(num_fragment))


def command_gen_graph():
    return ""


def gen_fold(num_paper, n_fold):
    list = np.arange(num_paper)  # generate array ex: 0,1,2,3,4,5,6,...,10
    np.random.shuffle(list)  # shuffle array ex: 2,8,6,7,10,9,1,3,5,4
    return np.split(list, n_fold)  # split the array ex [1,5,7],[4,3,2],[8,9,6],[0]


def execute(command):
    try:
        subprocess.check_output(command, shell=True)
    except subprocess.CalledProcessError as e:
        print(e.output)

def get_author_number(db_name):
    return int(db_name.split('_')[-3].split('a')[-1])

def cross(db_name, path, num_paper, n_fold):
    folds = gen_fold(num_paper, n_fold)
    print(folds)
    for key, fold in zip([i for i in range(0,len(folds))], folds):
        # print(folds)
        get_csv = command_get_csv(db_name, path + '/csv', fold, '_n'+str(key))
        print(get_csv)
        execute(get_csv)
    for root, dirs, files in os.walk(path + '/csv'):
        for file in files:
            file_path = root + '/' + file
            experiment = command_experiment(file_path, path+'/out', get_author_number(db_name)*num_paper/len(folds))
            print(experiment)
            execute(experiment)

def parser_args():
    parser = argparse.ArgumentParser(description='Get a stylometry synthetic data.')
    parser.add_argument('--num_paper', type=int, help='number of paper')
    parser.add_argument('--db_name', type=str, nargs='*', help="database name that want to get")
    parser.add_argument('--path', type=str, help="path running an experiment", default='.')
    parser.add_argument('--n_fold', type=int, help="number of fold in cross validation")
    return parser.parse_args()


if __name__ == '__main__':
    arg = parser_args()
    for db_name in arg.db_name:
        cross(db_name, arg.path, arg.num_paper, arg.n_fold)
