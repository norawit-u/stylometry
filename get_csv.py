import getpass
import os
import csv
import argparse
from lib2to3.btm_utils import tokens

import psycopg2
from six.moves import xrange

USER = getpass.getuser()  # database username
CON = None  # use only one connection

"""
    fragment_size: number of chunks in a fragment
    offset: number of chunks between n and n+1 fragment
    chunk_size: number of chunks
"""


def connect_database(db_name):
    """
        connecting to a database
        Args:
            db_name: database name

        Returns:
            con: database connection
            cur = cursor of the connection
    """
    global CON
    if not CON:
        CON = psycopg2.connect("dbname ='%s' user='%s' host=/tmp/" % (db_name, USER))
    cur = CON.cursor()
    return CON, cur


def is_fragmentable(fragment_size, offset, chunk_size):
    """
        function for checking is it possible to create a fragment of this size.
        Args:
            fragment_size: number of chunks in a fragment
            offset: number of chunks between n and n+1 fragment
            chunk_size: number of chunks
        Returns:
            True: if it is able to create a fragment with this parameter.
            False: else
    """
    return ((chunk_size - fragment_size) / offset) % 1 == 0


def get_fragments(fragment_size, offset, chunk_size):
    """
        get all the aviarable number of fragment from the parameter.
        Args:
            fragment_size: number of chunks in a fragment
            offset: number of chunks between n and n+1 fragment
            chunk_size: number of chunks

        Returns:
            List of aviarable number of fragment from the parameter.
    """
    if is_fragmentable(fragment_size, offset, chunk_size):
        return [tokens[x:x + fragment_size] for x in xrange(0, len(chunk_size), offset)]


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

def save_to_csv(list_return, name, fieldnames):
    """
        save data to csv file
        Args:
            list_return: a list of that we want to write to a csv file
            name: name of a csv file
            fieldnames: field names of the csv file (header)
    """
    os.makedirs(os.path.dirname(name + '.csv'), exist_ok=True)
    with open(name+'.csv', 'w') as csvfile:
        csvfile.write(','.join(map(str, field_names)))
        csvfile.write('\n')
        write = csv.writer(csvfile, delimiter=',')
        for x in range(0, len(list_return)):
            write.writerow(list_return[x])

def get_features(papers, chunk_size, num_chunk_per_fragment, offset, num_fragment, db_name):
    """
        get the feature from the database
        Args:
            papers: list of paper id
            num_chunk_per_fragment: number of chunks in a fragment
            offset: number of chunks between n and n+1 fragment
            num_fragment: number of fragment in a section
            db_name: database name
            chunk_size: number of chunk in a fragment
            papers: list of paper that wanted to get
        Returns:
            List of features
    """
    fragment_size = num_chunk_per_fragment # changing the name
    list_return = []  # for storing features that will return

    _, cur = connect_database(db_name)  # database connection and cursor
    for i in papers:  # loop for number of paper
        fragment_count = num_fragment * i # number of fragment(counter)
        for j in range(fragment_count,
                       fragment_count + num_fragment):  # loop from current fragment to current fragment + number of fragment
            chunk_id = i * chunk_size + offset * int(fragment_count / num_fragment)
            for k in range(chunk_id, chunk_id + fragment_size):
                list_feature = []
                list_feature.append(str(j))  # fragment id
                list_feature.append(str(i + 1))  # paper id
                list_feature.append(str(k + 1))  # chunk id
                cur.execute("SELECT value FROM features WHERE paper_id = '%s' AND chunk_id = '%s'", [i +1, k + 1])
                temp = cur.fetchall()
                for l in range(0, len(temp)):
                    list_feature.append(temp[l][0])
                list_return.append(list_feature)
    return list_return


def parser_args():
    parser = argparse.ArgumentParser(description='Get a stylometry synthetic data.')
    parser.add_argument('--fragment_size', type=int, help='number of chunks in a fragment')
    parser.add_argument('--num_fragment', type=int, help='number of fragment in a section')
    parser.add_argument('--chunk_size', type=int, help='number of chunk in a fragment')
    parser.add_argument('--num_chunk', type=int, help='number of chunk in a fragment')
    parser.add_argument('--num_chunk_per_section', type=int, help='number of chunk in a section')

    parser.add_argument('--papers', type=int, nargs='*', help='number of paper')
    parser.add_argument('--offset', type=int, help="number of chunks between n and n+1 fragment")
    parser.add_argument('--db_name', type=str, nargs='*', help="database name that want to get")
    parser.add_argument('--out_path', type=str, help="output path", default='.')
    parser.add_argument('--note', type=str, help="note for output file name")
    return parser.parse_args()


if __name__ == '__main__':
    field_names = ['fragment_id', 'paper_id', 'chunk_id']
    field_names.extend(['fragment_' + str(i) for i in range(1, 58)])
    arg = parser_args()
    for db_name in arg.db_name:
        if is_fragmentable(arg.fragment_size, arg.offset, arg.chunk_size):
            num_fragment = get_num_fragment(arg.fragment_size, arg.offset, arg.chunk_size)
            list_return = get_features(arg.papers, arg.chunk_size, arg.fragment_size, arg.offset, num_fragment, arg.db_name[0])
            save_to_csv(list_return, arg.out_path + "/" + arg.db_name[0]+arg.note, field_names)
        else:
            print('can not create a fragment')