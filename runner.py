import os
import argparse
import subprocess
import numpy as np
from subprocess import call


def command_get_csv(db_name, out_path, papers, note):
    """
    generate a command for running get_csv.py
    :param db_name: name of the database
    :param out_path: the path to save csv
    :param papers: list of paper ex: 1, 2, 3, 4, 5
    :param note: ending note
    :return: command for running get_csv.py
    """
    return "python get_csv.py --db_name %s --out_path %s --papers  %s --note %s" % (
        db_name, out_path, ' '.join(map(str, papers)), note)


def command_experiment(csv_path, output_path, num_fragment):
    """
    generate a command for running experiment.py
    :param csv_path: path to a csv file
    :param output_path: path where the experiment will be save
    :param num_fragment: number of fragment normally: author number * number of paper
    :return: command for running experiment.py
    """
    return "python experiment_old.py --csv_path %s --output_path %s --num_fragment %s" % (input, output_path, int(num_fragment))


def command_gen_graph(num_author, num_authors_list, papers, db_name, dir_path):
    """
    generate a command for running gengraph.py
    :param num_author:  number of author
    :param num_authors_list:  number of overall author
    :param papers: list of papers id
    :param db_name: database name
    :param dir_path: the output path of the experiment
    :return: command for running gengraph.py
    """
    return "python gengraph.py --num_authors %s  --num_authors_list %s --papers %s " \
           "--db_name %s  --dir_path %s" % (num_author, num_authors_list, ' '.join(map(str, papers)), db_name, dir_path)


def gen_fold(num_paper, n_fold, shuffle=False, append=False):
    doc_id_list = np.arange(num_paper)  # generate array ex: 0,1,2,3,4,5,6,...,10
    if shuffle:
        np.random.shuffle(doc_id_list)  # shuffle array ex: 2,8,6,7,10,9,1,3,5,4
    if append:
        tmp = np.array([])
        for i in range(1, n_fold):
            tmp[i] = doc_id_list[0:int(len(doc_id_list)/n_fold*i)] # ex: [1, 2], [1, 2, 3, 4], [1, 2, 3, 4, 5, 6]
        return tmp
    else:
        return np.split(doc_id_list, n_fold)  # split the array ex [1,5,7],[4,3,2],[8,9,6],[0]


def execute(command):
    try:
        return subprocess.check_output(command, shell=True)
    except subprocess.CalledProcessError as e:
        print(e.output)

def get_author_number(db_name):
    return int(db_name.split('_')[-3].split('a')[-1])

def get_author_list_number(db_name):
    return int(db_name.split('_')[-2].split('al')[-1])

def cross(db_name, path, num_paper, n_fold):
    folds = gen_fold(num_paper, n_fold)
    print(folds)
    for key, fold in enumerate(folds):
        # print(folds)
        get_csv = command_get_csv(db_name, path + '/csv', fold, '_n'+str(key))
        print(get_csv)
        execute(get_csv)
    for root, _, files in os.walk(path + '/csv'):
        for file in files:
            if db_name in file:
                file_path = root + '/' + file
                experiment = command_experiment(file_path, path+'/out', get_author_number(db_name)*num_paper/len(folds))
                print(experiment)
                execute(experiment)
    for key, fold in enumerate(folds):
        dir_path = path + '/out/' + db_name + '_n' + str(key) + '/'
        gengraph = command_gen_graph(get_author_number(db_name), get_author_list_number(db_name), fold, db_name, dir_path)
        print(gengraph)
        print(execute(gengraph))

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
