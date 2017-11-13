import os
import argparse
import subprocess
import numpy as np
from subprocess import call


def get_chunk_size(db_name):
    return int(int(db_name.split('_')[-4].split('t')[-1]) / int(db_name.split('_')[-1].split('sw')[-1]))


def command_get_csv(db_name, out_path, papers, fragment_size, offset, note):
    """
    generate a command for running get_csv.py
    :param offset:
    :param fragment_size:
    :param db_name: name of the database
    :param out_path: the path to save csv
    :param papers: list of paper ex: 1, 2, 3, 4, 5
    :param note: ending note
    :return: command for running get_csv.py
    """
    return "python get_csv.py --db_name %s --out_path %s --papers  %s --fragment_size %s --chunk_size %s --offset %s " \
           "--note %s" % (
           db_name, out_path, ' '.join(map(str, papers)), fragment_size, get_chunk_size(db_name), offset, note)


def command_experiment(csv_path, output_path):
    """
    generate a command for running experiment.py
    :param csv_path: path to a csv file
    :param output_path: path where the experiment will be save
    :param num_fragment: number of fragment normally: author number * number of paper
    :return: command for running experiment.py
    """
    return "python experiment_old.py --csv_path %s --output_path %s " % (
        csv_path, output_path)


def command_gen_graph(num_author, num_authors_list, papers, db_name, num_fragment, dir_path):
    """
    generate a command for running gengraph.py
    :param num_fragment:
    :param num_author:  number of author
    :param num_authors_list:  number of overall author
    :param papers: list of papers id
    :param db_name: database name
    :param dir_path: the output path of the experiment
    :return: command for running gengraph.py
    """
    if 'social' in db_name:
        return "python gengraph.py --num_authors %s  --num_authors_list %s --papers %s " \
               "--db_name %s --num_fragment %s --dir_path %s" % (
                   num_author, num_author, ' '.join(map(str, papers)), db_name,  num_fragment, dir_path)
    return "python gengraph.py --num_authors %s  --num_authors_list %s --papers %s " \
           "--db_name %s --dir_path %s" % (num_author, num_authors_list, ' '.join(map(str, papers)), db_name, dir_path)


def gen_fold(num_paper, n_fold, shuffle=False, append=False, train=False):
    """
    generate n fold id which slit the training set in to n subset ex: number of paper = 1000
    :param num_paper: number of paper
    :param n_fold: number of fold
    :param shuffle: want to shuffle the id or not
    :param append: want to extend each fold/subset or not
    :param train: want to generate data set - fold
    :return: array for fold example {[1,2,3],[4,5,6],[7,8,9]}
    """
    doc_id_list = np.arange(num_paper)  # generate array ex: 0,1,2,3,4,5,6,...,10
    if shuffle:
        np.random.shuffle(doc_id_list)  # shuffle array ex: 2,8,6,7,10,9,1,3,5,4
    if append:
        tmp = []
        for i in range(1, n_fold + 1):
            print(int(len(doc_id_list) / n_fold * i))
            tmp.append(doc_id_list[0:int(len(doc_id_list) / n_fold * i)])  # ex: [1, 2], [1, 2, 3, 4], [1, 2, 3, 4,
            # 5, 6]
        return tmp
    if train:
        tmp = []
        for i in range(n_fold):
            array = np.split(doc_id_list, n_fold)
            array.pop(i)
            tmp.append(np.concatenate(array))  # ex: [3,4,5,6,7,8],[1,2,5,6,7,8],[1,2,3,4,7,8]
        return tmp
    else:
        return np.split(doc_id_list, n_fold)  # split the array ex [1,5,7],[4,3,2],[8,9,6],[0]


def execute(command):
    """
    execute a bash command
    :param command: bash command
    :return: out put from an execution
    """
    try:
        return subprocess.check_output(command, shell=True)
    except subprocess.CalledProcessError as e:
        print(e.output)


def get_author_number(db_name):
    """
    gat a number of author from database name
    :param db_name: name of the database
    :return: number of author
    """
    return int(db_name.split('_')[-3].split('a')[-1])


def get_author_list_number(db_name):
    """
    gat a number of author list from database name
    :param db_name: database name
    :return: author list number
    """
    return int(db_name.split('_')[-2].split('al')[-1])


def cleanning(path):
    """
    clean working folder
    :param path: the path to remove
    :return: None
    """
    execute('rm -r' + path)


def get_num_fragment(fragment_size, offset, chunk_size):
    """
        get the number of fragment from parameter

        Args:
            fragment_size: number of chuncks in a fragment
            offset: number of chunks between n and n+1 fragment
            chunk_size: number of chunks
        Return:
            number of fragment
    """
    return int((chunk_size - fragment_size) / offset + 1)


def cross(db_name, path, num_paper, n_fold, fragment_size, offset, shuffle, append, clean=False):
    """
    apply cross validation and run the experiment
    :param offset:
    :param fragment_size:
    :param db_name: name of a database
    :param path: path for running experiment
    :param num_paper: number of paper
    :param n_fold: number of fold
    :param shuffle: shuffle the data or not
    :param append: append each fold or not
    :param clean: clean after run or not
    :return: None
    """
    folds = gen_fold(num_paper, n_fold, shuffle, append)
    print(folds)
    for key, fold in enumerate(folds):
        # print(folds)
        get_csv = command_get_csv(db_name, path + '/csv', fold, fragment_size, offset, '_n' + str(key))
        print(get_csv)
        execute(get_csv)
    for root, _, files in os.walk(path + '/csv'):
        for file in files:
            if str(db_name) in str(file):
                file_path = root + '/' + file
                experiment = command_experiment(file_path, path + '/out')
                print(experiment)
                execute(experiment)
    for key, fold in enumerate(folds):
        dir_path = path + '/out/' + db_name + '_n' + str(key) + '/'
        gengraph = command_gen_graph(get_author_number(db_name), get_author_list_number(db_name), [(x + 1) for x in fold], db_name,
                                     get_num_fragment(fragment_size, offset, get_chunk_size(db_name)), dir_path)
        print(gengraph)
        print(str(execute(gengraph)))
        print("============")
    if clean:
        cleanning(path)


def parser_args():
    """
    init argument parsing
    :return: argument
    """
    parser = argparse.ArgumentParser(description='Get a stylometry synthetic data.')
    parser.add_argument('--num_paper', type=int, help='number of paper')
    parser.add_argument('--db_name', type=str, nargs='*', help="database name that want to get")
    parser.add_argument('--path', type=str, help="path running an experiment", default='.')
    parser.add_argument('--n_fold', type=int, help="number of fold in cross validation")
    parser.add_argument('-shuffle', type=bool, default=False, help='shuffle a cross validation')
    parser.add_argument('-append', type=bool, default=False, help='append the fold')
    parser.add_argument('-clean', type=bool, default=False, help='clean after finish running')
    parser.add_argument('--fragment_size', type=int, default=False, help='number of chunk in fragment')
    parser.add_argument('--offset', type=int, default=False, help='number of chunk between chunk n and n+1')
    return parser.parse_args()


if __name__ == '__main__':
    arg = parser_args()
    for db_name in arg.db_name:
        cross(db_name, arg.path, arg.num_paper, arg.n_fold, arg.fragment_size, arg.offset, arg.shuffle, arg.append, arg.clean)
