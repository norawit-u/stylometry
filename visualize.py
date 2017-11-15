import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import ast
import argparse

PATH = "syn_eng_max_while_np1000_c600_t8000_a2_al2_sw200_n0/"



def get_input(path):
    l = []
    for root, dir, files in os.walk(path):
        for file in files:
            with open(root + file, 'r') as f:
                content = f.read().replace('\n', '')
                tmp = []
                x = ast.literal_eval(content)
                for i in range(1, len(x)):
                    l.append(int(x[i][1]))
    return l

def plot(x, name):
    fig = plt.hist(x, normed=0)
    plt.savefig(name+'.png')

def parser_args():
    """
    init argument parsing
    :return: argument
    """
    parser = argparse.ArgumentParser(description='print histogram')
    parser.add_argument('--path', type=str, help='path to a folder')
    return parser.parse_args()


if __name__ == "__main__":
    arg = parser_args()
    data = get_input(arg.path)
    plot(data, arg.path.split('/')[-1])
