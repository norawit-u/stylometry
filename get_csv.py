import csv
import getpass
import argparse
import psycopg2

USER = getpass.getuser()  # database username
CON = None  # use only one connection

"""
    fragment_size: number of chuncks in a fragment
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
            cur = cursor of the connetion
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
            fragment_size: number of chuncks in a fragment
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
            fragment_size: number of chuncks in a fragment
            offset: number of chuks between n and n+1 fragment
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


def save_to_csv(list_return, name):
    """
        save data to csv file
        Args:
            list_return: a list of that we want to write to a csv file
            name: name of a csv file
    """
    with open(name+'.csv', 'w') as csvfile:
        write = csv.writer(csvfile, delimiter=',')
        for x in range(0, len(list_return)):
            write.writerow(list_return[x])


def get_features(num_paper, fragment_size, offset, num_fragment, db_name):
    """
        get the feature from the database
        Args:
            num_paper: number of paper
            fragment_size: number of chuncks in a fragment
            offset: number of chunks between n and n+1 fragment
            num_fragment: number of fragment in a section
            db_name: database name
        Returns:
            List of features
    """
    list_return = []  # for storing features that will return
    fragment_count = 1  # number of fragment(counter)
    chunk_count = 1 + fragment_size  # number of chunk(counter)
    chunk_number = 1  # chunk id
    _, cur = connect_database(db_name)  # database connection and cursor
    for i in range(0, num_paper):  # loop for number of paper
        for j in range(fragment_count,
                       fragment_count + num_fragment):  # loop from current fragment to current fragment + number of fragment
            chunk_count -= fragment_size
            for k in range(chunk_count, chunk_count + fragment_size):
                list_feature = []
                list_feature.append(str(j))  # fragment id
                list_feature.append(str(i + 1))  # paper id
                list_feature.append(str(chunk_number))  # chunk id
                cur.execute("SELECT value FROM features WHERE paper_id = '%s' AND chunk_id = '%s'", [i + 1, k])
                temp = cur.fetchall()
                for l in range(0, len(temp)):
                    list_feature.append(temp[l][0])
                list_return.append(list_feature)
                chunk_count += 1
                chunk_number += 1
            chunk_count += offset
        chunk_count += fragment_size - offset
        fragment_count += num_fragment
    return list_return


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


if __name__ == '__main__':
    arg = parser_args()
    if is_fragmentable(arg.fragment_size, arg.offset, arg.chunk_size):
        num_fragment = get_num_fragment(arg.fragment_size, arg.offset, arg.chunk_size)
        print(num_fragment)
        list_return = get_features(arg.num_paper, arg.fragment_size, arg.offset, num_fragment, arg.db_name[0])
        save_to_csv(list_return, arg.out_path+"/"+arg.db_name[0])
