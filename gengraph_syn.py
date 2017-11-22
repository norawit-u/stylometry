import psycopg2
import ast
import operator
import math
import argparse
from scipy import stats


class GenGraph:
    def __init__(self, num_authors, num_authors_list, papers, db_name, fname, num_fragment):
        """
        init GenGraph
        :param num_authors: number of author in the paper
        :param num_authors_list:  number of all the author in the paper
        :param papers: list of paper
        :param db_name: name of the database
        :param fname: directory path of the experiment out put
        :param num_fragment:  number of fragment in a paper
        """
        self.num_authors = num_authors
        self.num_authors_list = num_authors_list
        self.papers = papers
        self.db_name = db_name
        self.fname = fname
        self.num_fragment = num_fragment

    def get_authors_list(self, paper_id):
        """
        get all the authors id who write a paper
        :param paper_id: paper id
        :return: list of author id
        """
        con = psycopg2.connect("dbname ='%s' user='cpehk01' host=/tmp/" % (self.db_name.lower()))
        cur = con.cursor()
        cur.execute("SELECT author_id FROM writes_hidden WHERE paper_id = '%s' " % (paper_id))
        get_list = cur.fetchall()
        list_return = []
        for i in range(0, self.num_authors_list):
            list_return.append(get_list[i][0])
        return list_return

    def fit_author_to_fragment(self, fragment_size, author_list):
        """
         Use round robin to circulate the author list
        :param fragment_size:
        :param author_list:
        :return: list of author who write the fragment
        """
        fragment_author_list = []
        for i in range(fragment_size):
            fragment_author_list.append(author_list[i % len(author_list)])
        return sorted(fragment_author_list)

    def generate_paper(self):
        """
        generate paper
        :return:
        """
        papers = {}
        for j in self.papers:
            paper_id = j
            new_fragments = {}
            author_list = self.get_authors_list(str(paper_id))  # query authors_list
            for i, author_id in enumerate(
                    self.fit_author_to_fragment(self.num_fragment, author_list[:self.num_authors])):
                fragment_id = i + self.num_fragment * (j - 1) + 1
                new_fragments[fragment_id] = author_id  # frag_id = author_list[i]
            papers[paper_id] = {'authors': author_list, 'fragments': new_fragments}
        # print(papers)
        return papers

    def generate_frag_probs(self, papers):
        """
        generate a fragment probability
        :param papers: dict of paper_id with author id and fragment id
        :return: dict of paper_id with author id and dict fragment of probability
        """
        frag_probs = {}
        for (paper_id, v) in papers.items():
            authors = v['authors'][0:self.num_authors_list]
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
        """
        get a similar fragments from the experiment
        :param papers: array contain fragment and author information
        :param paper_id: id of a paper
        :param fragment_id: id of a fragment
        :return:
        """
        similar_fragments = []
        author_id = papers[paper_id]['fragments'][fragment_id]
        fname = self.fname + "%s" % fragment_id
        # print(paper_id, fragment_id)
        # read file
        with open(fname, 'r') as f:
            content = f.read().replace('\n', '')
        x = ast.literal_eval(content)  # convert content in file to python object
        for i in range(1, len(x)):
            fragment_id2 = int(x[i][1])  # get fragment id
            # print(fragment_id2, self.num_authors)
            paper_id2 = int((fragment_id2 - 1) / self.num_fragment) + 1  # compute paper id
            similar_fragments.append((paper_id2, fragment_id2, author_id))
        # print("similar_fragments", similar_fragments)
        # print("tmp_fragment", tmp_fragment)
        return similar_fragments

    def recalculate_prob(self, papers, frag_probs, paper_id, fragment_id):
        """
        recalculate the probability from the experiment output
        :param papers: array contain fragment and author information
        :param frag_probs: fragment probability
        :param paper_id: id of the paper
        :param fragment_id: id of the fragment
        :return:
        """
        similar_fragments = self.get_similar_fragments(papers, paper_id, fragment_id)
        authors_of_interest = papers[paper_id]['authors']
        sum_pmf = {k: 0 for k in authors_of_interest}
        num_pmfs = 0
        # print(sum_pmf)
        # print("similar_fragments", similar_fragments)
        # print("fragment_id", fragment_id)
        # print("frag_probs", frag_probs)
        # print("==================================")
        for entry in similar_fragments:
            p_id, f_id = entry[0], entry[1]
            # print(p_id, f_id)
            # create a new pmf by using a paper id and fragment id from the experiment out put
            # to get a author id and pmf from the frag_prob
            pmf = frag_probs[p_id][f_id]
            # get author id that is in the ground truth
            new_pmf = {k: v for k, v in pmf.items() if k in authors_of_interest}
            # print(pmf, new_pmf, authors_of_interest, p_id, f_id, entry[2])
            if len(new_pmf) > 0 and sum(new_pmf.values()) != 0:
                # print(new_pmf, authors_of_interest, entry[0], entry[1], entry[2])
                total_prob = sum(new_pmf.values())
                new_pmf = {k: v / total_prob for k, v in new_pmf.items()}
                sum_pmf = {k: sum_pmf.get(k, 0) + new_pmf.get(k, 0) for k in set(sum_pmf)}
                num_pmfs = num_pmfs + 1
                # if new_pmf:
                #     print("new_pmf", new_pmf)
                #     print(p_id, f_id)
                # print(p_id, f_id, pmf, new_pmf, sum_pmf, num_pmfs, total_prob)
        # print("new_pmf", new_pmf)
        # print("sum_pmf", sum_pmf)
        # print("num_pmfs", num_pmfs)
        if num_pmfs > 0:
            avg_pmf = {k: v / num_pmfs for k, v in sum_pmf.items()}
        else:
            avg_pmf = frag_probs[paper_id][fragment_id]
        # print('avg_pmf', avg_pmf)
        return avg_pmf

    def recalculate_frag_probs(self, papers, frag_probs):
        """
        recalculate a fragment probability
        :param papers: array contain fragment and author information
        :param frag_probs: fragment probability
        :return:
        """
        new_frag_probs = {}
        # update the pmf for each paper and fragment
        for paper_id, frag_pmfs in frag_probs.items():
            new_frag_pmfs = {}
            for frag_id in frag_pmfs.keys():
                # print(frag_id)
                avg_pmf = self.recalculate_prob(papers, frag_probs, paper_id, frag_id)
                # print(avg_pmf)
                new_frag_pmfs[frag_id] = avg_pmf
            new_frag_probs[paper_id] = new_frag_pmfs
            # print(frag_id, new_frag_probs[paper_id])
        return new_frag_probs

    def checking_accuracy_fragments(self, papers, frag_probs):
        """
        checking the accuracy for each fragment
        :param papers: array contain fragment and author information
        :param frag_probs: fragment probability
        :return:
        """
        # total_fragment = sum([len(papers[i]['fragments'].keys()) for i in papers])
        # sum the probability in each fragment
        total_fragment = sum([len(frag_probs[i].keys()) for i in frag_probs.keys()])
        list_check = {}
        sum_prob = {}
        top_prob = {}
        for x in papers:
            if frag_probs[x]:
                # print("frag_probs", frag_probs[x])
                sum_prob[x] = {k: 0 for k in frag_probs[x][sorted(list(frag_probs[x].keys()))[0]]}
                # print(list(frag_probs[x].keys()))
                # print(frag_probs[x][list(frag_probs[x].keys())[0]])
                # print("sum_prob", sum_prob[x])
                for y in list(frag_probs[x].keys()):
                    sum_prob[x] = {k: sum_prob[x][k] + v for k, v in frag_probs[x][y].items()}
                # print("sum_prob loop", sum_prob[x])
                sum_prob[x] = {k: sum_prob[x][k] / self.num_authors for k in
                               frag_probs[x][list(frag_probs[x].keys())[0]]}

                temp_top = {k: sorted(v.items(), key=operator.itemgetter(1))[-1][0] for k, v in frag_probs[x].items()}
                sum_top = {}
                for k, v in temp_top.items():
                    if v not in sum_top:
                        sum_top[v] = 0
                    sum_top[v] += 1
                print(sum_top)
                top_prob[x] = sum_top
                # print("sum_prob final", sum_prob[x])
        # sort the prob and pick top n author
        print(top_prob)
        for key, z in enumerate(sum_prob):
            list_check[z] = sorted(sum_prob[z].items(),  key=operator.itemgetter(1), reverse=True)[
                            0:self.num_authors]
            # print(list_check[z])
        # print(list_check)
        # count all the collect author in the paper
        count_all = 0
        count = 0
        count_least_1 = 0
        accuracy_list = []
        for i in papers.keys():
            # print('i', i)
            count_tmp = 0
            for k in range(0, len(top_prob[i])):
                # print("k", k)
                # print("author_id", author_id, 'list_check[i][k][0]', list_check[i][k][0])
                # print("author_id", author_id, 'list_check[%s][%s][%s]'%(i,k,0), list_check[i][k][0])
                if top_prob[k][0] in papers[i]['authors'][:self.num_authors]:
                    count += 1
                    count_tmp += 1
                    # print(author_id, list_check[i])
            accuracy_list.append(count_tmp)
            if count_tmp == len(frag_probs[i].keys()):
                count_all += 1
            if count_tmp >= 1:
                count_least_1 += 1
        print(count)
        print(accuracy_list)
        print("Accuracy all true: %s" % (float(count_all * 100 / len(papers))))
        print("Accuracy true at least 1 : %s" % (float(count_least_1 * 100 / len(papers))))
        print("Accuracy: %s" % (float(count * 100 / (len(papers) * self.num_authors))))

        #
        # old code not use, will be move in the future
        # count_all = 0
        # count = 0
        # count_least_1 = 0
        # for i in papers.keys():
        #     # print('i', i)
        #     count_tmp = 0
        #     for fragment_id, author_id in sorted(papers[i]['fragments'].items(), key=operator.itemgetter(0)):
        #         # print('j', j)
        #         author_id = papers[i]['fragments'][fragment_id]
        #         # print(frag_probs[i])
        #         if fragment_id in frag_probs[i].keys():
        #             # print(sorted(sorted(frag_probs[i][fragment_id].items(), key=operator.itemgetter(0),reverse=False), key=operator.itemgetter(1), reverse=True))
        #             # print(author_id,sorted(sorted(frag_probs[i][fragment_id].items(), key=operator.itemgetter(0), reverse=False), key = operator.itemgetter(1), reverse = True))
        #             experiment_author_id = sorted(sorted(frag_probs[i][fragment_id].items(), key=operator.itemgetter(0),
        #                                                  reverse=False), key=operator.itemgetter(1), reverse=True)[0][0]
        #             if author_id == experiment_author_id:
        #                 count += 1
        #                 count_tmp += 1
        #     if count_tmp == len(papers[i]['fragments'].keys()):
        #         count_all += 1
        #     if count_tmp >= 1:
        #         count_least_1 += 1
        # print(count)
        # print("Accuracy all true: %s" % (float(count_all * 100 / len(papers))))
        # print("Accuracy true at least 1 : %s" % (float(count_least_1 * 100 / len(papers))))
        # print("Accuracy: %s" % (float(count * 100 / total_fragment)))

    def sum_prob(self, papers, frag_probs):
        """
        some the probability
        :param papers: array contain fragment and author information
        :param frag_probs: fragment probability
        :return:
        """
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
            sum_prob[i] = {key: sum_prob[i][key] / self.num_authors for key in authors_interest}
            sorted_prob = sorted(sum_prob[i].items(), key=operator.itemgetter(1))
            # print("paper %s prob %s" % (i, sorted_prob))

    def max_entropy(self, frag_probs, paper_id):
        """
        calculate max entropy
        :param frag_probs: fragment probability
        :param papers: array contain fragment and author information
        :return: max entropy
        """
        entropy = {}
        for i in frag_probs[paper_id].keys():
            tmp_entropy = 0
            for j in frag_probs[paper_id][i].keys():
                if frag_probs[paper_id][i][j] != 0:
                    tmp_entropy += (-1) * frag_probs[paper_id][i][j] * math.log(frag_probs[paper_id][i][j])
            entropy[i] = tmp_entropy
        max_entropy = max(entropy.items(), key=operator.itemgetter(1))[0]
        return max_entropy

    def entropy(self, frag_probs, paper_id):
        """
        calculate entropy using stats
        :param frag_probs: fragment probability
        :param papers: array contain fragment and author information
        :return:
        """
        entropy = {}
        for i in frag_probs[paper_id].keys():
            entropy[i] = stats.entropy(list(frag_probs[paper_id][i].values()))
        return entropy

    def remove_high_entropy(self, frag_probs, papers, percent=90, num=0, per_dataset=False):
        """
        Remove a high entropy fragment for a paper
        :param papers: array contain fragment and author information
        :param frag_probs: fragment probability
        :param percent: percent of removing
        :param per_dataset: use an entropy of the data set
        :return:
        """

        over_all_entropy = []
        for i in [self.entropy(frag_probs, j) for j in papers]:
            over_all_entropy.extend(list(i.values()))
        sorted(over_all_entropy)
        upper_bound = over_all_entropy[int(len(over_all_entropy) * percent / 100)]
        # print(upper_bound)
        for paper_id in papers:
            entropys = self.entropy(frag_probs, paper_id)
            entropys_len = len(entropys)
            for key, entropy in enumerate(sorted(entropys.items(), key=operator.itemgetter(1))):
                if num != 0:  # remove by number of fragment
                    if key >= entropys_len - num:
                        del frag_probs[paper_id][entropy[0]]
                elif not per_dataset:  # remove by percent of fragment
                    if entropy[1] >= sorted(entropys.items(), key=operator.itemgetter(1))[
                        int(len(entropys.items()) * percent / 100)][1]:
                        del frag_probs[paper_id][entropy[0]]
                else:  # remove by over all corpus
                    if entropy[1] >= upper_bound:
                        # print(paper_id,entropy[0],entropy[1])
                        del frag_probs[paper_id][entropy[0]]


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
    parser.add_argument('--entropy', type=int,
                        help='if use entropy the program will remove high entropy fragment')
    parser.add_argument('--percent_entropy', type=bool,
                        help='use percent of fragment instead of number of fragment')
    return parser.parse_args()


if __name__ == "__main__":
    arg = parser_args()
    gengraph = GenGraph(arg.num_authors, arg.num_authors_list, arg.papers, arg.db_name, arg.dir_path, arg.num_fragment)
    papers = gengraph.generate_paper()
    # print(papers)
    frag_probs = gengraph.generate_frag_probs(papers)

    for i in range(0, 1):  # recalculate the graph
        new_frag_probs = gengraph.recalculate_frag_probs(papers, frag_probs)
        frag_probs = new_frag_probs
        # print(i)

    gengraph.sum_prob(papers, frag_probs)
    if arg.entropy:  # if use entropy
        # print(frag_probs)
        if arg.percent_entropy:
            gengraph.remove_high_entropy(frag_probs, papers, percent=arg.entropy)
        else:
            gengraph.remove_high_entropy(frag_probs, papers, num=arg.entropy)
        print("use entropy")
    # print(frag_probs)
    # check the accuracy
    gengraph.checking_accuracy_fragments(papers, frag_probs)
