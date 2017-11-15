import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot
import matplotlib.pyplot as plt
import numpy as np
import os
import ast

PATH = "syn_eng_max_while_np1000_c600_t8000_a2_al2_sw200_n0/"



def get_input():
    l = []
    for root, dir, files in os.walk(PATH):
        for file in files:
            with open(root + file, 'r') as f:
                content = f.read().replace('\n', '')
                tmp = []
                x = ast.literal_eval(content)
                for i in range(1, len(x)):
                    l.append(int(x[i][1]))
    return l

def plot(x):
    fig = plt.hist(x, normed=0)
    plt.savefig('temp.png')

if __name__ == "__main__":
    data = get_input()
    plot(data)
