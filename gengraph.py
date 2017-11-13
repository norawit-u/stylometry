import numpy as np
import psycopg2
import ast
import operator
import math
import argparse


class Gengraph:
    def __init__(self, num_authors, num_authors_list, papers, db_name, fname, num_fragment):
        self.num_authors = num_authors
        self.num_authors_list = num_authors_list
        self.papers = papers
        self.db_name = db_name
        self.fname = fname
        self.num_fragment = num_fragment

    def get_authors_list(self, paper_id):
        con = psycopg2.connect("dbname ='%s' user='cpehk01' host=/tmp/" % (self.db_name.lower()))
        cur = con.cursor()
        cur.execute("SELECT author_id FROM writes_hidden WHERE paper_id = '%s' " % (paper_id))
        get_list = cur.fetchall()
        list_return = []
        for i in get_list:
            list_return.append(i[0])
        return list_return

        # TODO generate fragment from paper

    # TODO flexible token size
    def fit_author_to_fragment(self, fragment_size, author_list):
        '''
         Use round robin to circulate the author list
        :param fragment_size:
        :param author_list:
        :return:
        '''
        fragment_author_list = []
        for i in range(fragment_size):
            fragment_author_list.append(author_list[i % len(author_list)])
        return fragment_author_list

    def generate_paper(self):
        papers = {}
        for j in self.papers:  # loop over the papers
            paper_id = j
            new_fragments = {}
            author_list = self.get_authors_list(str(paper_id))  # query authors_list
            for i, author_id in enumerate(self.fit_author_to_fragment(self.num_fragment, author_list)):
                fragment_id = i + self.num_fragment * (j - 1) + 1
                new_fragments[fragment_id] = author_id  # frag_id = author_list[i]
            papers[paper_id] = {'authors': author_list, 'fragments': new_fragments}
        # print(papers)
        return papers

    def generate_frag_probs(self, papers):
        frag_probs = {}
        for (paper_id, v) in papers.items():
            # print(v)
            authors = v['authors']
            # print(authors)
            # print(self.get_authors_list(str(paper_id)))
            prob = 1.0 / len(authors)
            uniform_pmf = {k: prob for k in authors}
            fragments = v['fragments']
            new_fragments_pmfs = {}
            for fragment_id in fragments.keys():
                new_fragments_pmfs[fragment_id] = dict(uniform_pmf)
            frag_probs[paper_id] = new_fragments_pmfs
        # print(frag_probs)
        return frag_probs

    def get_similar_fragments(self, papers, paper_id, fragment_id):
        similar_fragments = []
        author_id = papers[paper_id]['fragments'][fragment_id]
        fname = self.fname + "%s" % fragment_id
        with open(fname, 'r') as f:
            content = f.read().replace('\n', '')
        x = ast.literal_eval(content)
        for i in range(1, len(x)):
            fragment_id2 = int(x[i][1])
            # print(fragment_id2, self.num_authors)
            paper_id2 = int((fragment_id2 - 1) / self.num_fragment) + 1
            similar_fragments.append((paper_id2, fragment_id2, author_id))
        return similar_fragments

    def recalculate_prob(self, papers, frag_probs, paper_id, fragment_id):
        similar_fragments = self.get_similar_fragments(papers, paper_id, fragment_id)
        # print(similar_fragments)
        authors_of_interest = papers[paper_id]['authors']
        sum_pmf = {k: 0 for k in authors_of_interest}
        num_pmfs = 0
        # print("similar_fragments",similar_fragments)
        # print("fragment_id",fragment_id)
        # print("frag_probs",frag_probs)
        # print("==================================")
        for entry in similar_fragments:
            p_id, f_id = entry[0], entry[1]
            # print(p_id, f_id)
            pmf = frag_probs[p_id][f_id]
            new_pmf = {k: v for k, v in pmf.items() if k in authors_of_interest}
            if len(new_pmf) > 0 and sum(new_pmf.values()) != 0:
                total_prob = sum(new_pmf.values())
                new_pmf = {k: v / total_prob for k, v in new_pmf.items()}
                sum_pmf = {k: sum_pmf.get(k, 0) + new_pmf.get(k, 0) for k in set(sum_pmf)}
                num_pmfs = num_pmfs + 1
        if num_pmfs > 0:
            avg_pmf = {k: v / num_pmfs for k, v in sum_pmf.items()}
        else:
            avg_pmf = frag_probs[paper_id][fragment_id]
        return avg_pmf

    def recalculate_frag_probs(self, papers, frag_probs):
        new_frag_probs = {}
        for paper_id, frag_pmfs in frag_probs.items():
            new_frag_pmfs = {}
            for frag_id in frag_pmfs.keys():
                # print(frag_id)
                avg_pmf = self.recalculate_prob(papers, frag_probs, paper_id, frag_id)
                new_frag_pmfs[frag_id] = avg_pmf
            new_frag_probs[paper_id] = new_frag_pmfs
        return new_frag_probs

    def checking_accuracy_fragments(self, papers, frag_probs):
        list_check = {}
        sum_prob = {}
        for x in papers:
            # print(frag_probs)
            # print(frag_probs[x])
            sum_prob[x] = {k: 0 for k in frag_probs[x][self.num_fragment * x]}
            for y in frag_probs[x].keys():
                sum_prob[x] = {k: sum_prob[x][k] + v for k, v in frag_probs[x][y].items()}
            sum_prob[x] = {k: sum_prob[x][k] / len(papers[x]['authors']) for k in
                           frag_probs[x][self.num_fragment * x]}
        for key, z in enumerate(sum_prob):
            list_check[self.papers[key]] = sorted(sum_prob[z].items(), key=operator.itemgetter(1), reverse=True)[
                                           0:len(papers[z]['authors'])]
        count_all = 0
        count = 0
        count_least_1 = 0
        for i in papers.keys():
            count_tmp = 0
            for j in papers[i]['fragments'].keys():
                author_id = papers[i]['fragments'][j]
                for k in range(0, len(list_check[i])):
                    if author_id == list_check[i][k][0]:
                        count += 1
                        count_tmp += 1
            if count_tmp == len(papers[i]['authors']):
                count_all += 1
            if count_tmp >= 1:
                count_least_1 += 1
        print(count)
        print(count_least_1)
        print(count_all)
        print("Accuracy alls true: %s" % (float(count_all * 100 / len(papers))))
        print("Accuracy ture at least 1 : %s" % (float(count_least_1 * 100 / len(papers))))
        print("Accuracy: %s" % (float(count * 100 / (len(papers) * len(papers[i]['authors'])))))

    def sum_prob(self, papers, frag_probs):
        sum_prob = {}
        for i in frag_probs.keys():
            authors_interest = []
            for j in frag_probs[i].keys():
                for k in frag_probs[i][j].keys():
                    if k not in authors_interest:
                        authors_interest.append(k)
            sum_prob[i] = {k: 0 for k in authors_interest}

            for x in frag_probs[i].keys():
                for y in frag_probs[i][x].keys():
                    sum_prob[i][y] = sum_prob[i][y] + frag_probs[i][x][y]
            sum_prob[i] = {key: sum_prob[i][key] / len(papers[i]['authors']) for key in authors_interest}
            sorted_prob = sorted(sum_prob[i].items(), key=operator.itemgetter(1), reverse=True)
            # print("paper %s prob %s" % (i + 1, sorted_prob))

    def max_entropy(self, frag_probs, paper_id, base=10):
        entropy = {}
        for i in frag_probs[paper_id].keys():
            # tmp_entropy = 0
            # for j in frag_probs[paper_id][i].keys():
            #     if frag_probs[paper_id][i][j] != 0:
            #         tmp_entropy += (-1) * frag_probs[paper_id][i][j] * math.log(frag_probs[paper_id][i][j])
            # entropy[i] = tmp_entropy
            entropy[i] = -np.sum(frag_probs[paper_id][i] * np.log(frag_probs[paper_id][i].values()), axis=0)
        max_entropy = max(entropy.items(), key=operator.itemgetter(1))[0]
        return max_entropy

def parser_args():
    parser = argparse.ArgumentParser(description='Create a stylometry synthetic dataset.')

    parser.add_argument('--num_authors', type=int,
                        help='number of rea l authors')
    parser.add_argument('--num_authors_list', type=int,
                        help='number of authors including generated one')
    parser.add_argument('--papers', type=int, nargs='*',
                        help='papers id')
    parser.add_argument('--num_paper', type=int,
                        help='number of paper')
    parser.add_argument('--num_fragment', type=int,
                        help='number of fragment in the paper')
    parser.add_argument('--db_name', type=str,
                        help='database basename')
    parser.add_argument('--dir_path', type=str,
                        help='path to the directory')
    return parser.parse_args()


if __name__ == "__main__":
    arg = parser_args()
    gengraph = Gengraph(arg.num_authors, arg.num_authors_list, arg.papers, arg.db_name, arg.dir_path, arg.num_fragment)
    papers = gengraph.generate_paper()
    frag_probs = gengraph.generate_frag_probs(papers)

    for i in range(0, 10):
        new_frag_probs = gengraph.recalculate_frag_probs(papers, frag_probs)
        frag_probs = new_frag_probs
        # print(i)
    for paper_id in papers:
        print(gengraph.max_entropy(frag_probs, paper_id))
    gengraph.sum_prob(papers, frag_probs)
    gengraph.checking_accuracy_fragments(papers, frag_probs)
