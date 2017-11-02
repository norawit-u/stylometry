import csv
import argparse
import psycopg2

def save_to_csv(list_return, name, fieldnames):
    """
        save data to csv file
        Args:
            list_return: a list of that we want to write to a csv file
            name: name of a csv file
            fieldnames: field names of the csv file (header)
    """
    with open(name + '.csv', 'w') as csvfile:
        for name in fieldnames:
            csvfile.write(name + ',')
        write = csv.writer(csvfile, delimiter=',')
        for x in range(0, len(list_return)):
            write.writerow(list_return[x])


def get_syn(db_name, chunk_size, author_number, num_paper):
    """
        get the feature from the database
        Args:
            db_name: database name
            chunk_size: number of chunk in a fragment
            author_number: number of author in paper
            num_paper: number of paper
        Returns:
            List of features
    """
    con = psycopg2.connect("dbname ='%s' user='cpehk01' host=/tmp/" % (db_name))
    cur = con.cursor()
    print(db_name + " " + str(chunk_size) + " " + str(author_number))
    list_return = []

    chunk_num = 1
    for i in range(0, num_paper):  # number papers
        for j in range(0, chunk_size):  # number chunks per paper (token_size/chunk_size)
            list_feature = []
            chunk_per_fragment = 0
            try:
                chunk_per_fragment = (j / (chunk_size / author_number))
            except ZeroDivisionError:
                print('error: didided by 0')
            list_feature.append(str((chunk_per_fragment + 1) + (
            author_number * i)))  # first number is chunk per fragment, and the last number is number of authors (aka a2)
            # list_feature.append(str(((j/10)+1)+(3*i)))
            # print(str(chunk_per_fragment))
            # print(str(chunk_per_fragment + 1) + " " + str(author_number * i))
            list_feature.append(str(i + 1))
            list_feature.append(str(chunk_num))
            cur.execute("SELECT value FROM features WHERE paper_id = '%s' AND chunk_id = '%s'", [i + 1, chunk_num])
            temp = cur.fetchall()
            for k in range(0, len(temp)):
                list_feature.append(temp[k][0])
            list_return.append(list_feature)
            chunk_num += 1
    return list_return


def parser_args():
    parser = argparse.ArgumentParser(description='Get a stylometry synthetic data.')
    parser.add_argument('--num_paper', type=int, help='number of paper')
    parser.add_argument('--db_name', type=str, nargs='*', help="database name that want to get")
    parser.add_argument('--out_path', type=str, help="output path", default='.')
    return parser.parse_args()


if __name__ == '__main__':
    field_names = ['fragment_id', 'paper_id', 'chunk_id']
    field_names.extend(['fragment_' + str(i) for i in range(1, 58)])
    arg = parser_args()
    for db_name in arg.db_name:
        list_return = get_syn(db_name, int(int(db_name.split('_')[-4].split('t')[-1]) / int(
            db_name.split('_')[-1].split('sw')[-1])),
                              int(db_name.split('_')[-3].split('a')[-1]),
                              int(db_name.split('_')[-6].split('np')[-1]))
    save_to_csv(list_return, arg.out_path + "/" + str(arg.fragment_size) + '_' + arg.db_name[0], field_names)
