import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot
import matplotlib.pyplot as plt
import numpy as np
import os
import ast

PATH = " test/out_tmp/syn_eng_max_while_np1000_c600_t8000_a2_al2_sw200_n4/"



def get_input():
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


def plot(x):
    fig = plt.figure()
    n, bins, patches = plt.hist(x)
    fig.savefig('temp.png')

if __name__ == "main":
    data = get_input()
    plot(data)
