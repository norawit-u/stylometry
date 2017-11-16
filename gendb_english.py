import nltk
import argparse
import numpy as np
import getpass
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from paragraph import Paragraph
from collections import Counter

from six.moves import xrange

###############################################
#                 PYTHON 2                    #
###############################################
class Syntactic:
    def __init__(self, chunk_size, token_size, num_authors, num_authors_list, sliding_window, num_paper):
        self.chunk_size = chunk_size
        self.token_size = token_size
        self.num_authors = num_authors
        self.num_authors_list = num_authors_list
        self.sliding_window = sliding_window
        self.db_name = "syn_eng_max_while_np%s_c%s_t%s_a%s_al%s_sw%s" % (
            num_paper, chunk_size, token_size, num_authors, num_authors_list, sliding_window)
        self.num_paper = num_paper

    def create_db_table(self):
        con = psycopg2.connect("dbname ='postgres' user='%s' host=/tmp/" % (getpass.getuser()))
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute("DROP DATABASE IF EXISTS " + self.db_name)
        cur.execute("CREATE DATABASE " + self.db_name)
        con.close()
        cur.close()
        con = psycopg2.connect("dbname ='%s' user='%s' host=/tmp/" % (self.db_name.lower(), getpass.getuser()))
        cur = con.cursor()

        cur.execute("DROP TABLE IF EXISTS author CASCADE")
        cur.execute("DROP TABLE IF EXISTS paper CASCADE")
        cur.execute("DROP TABLE IF EXISTS writes_hidden CASCADE")
        cur.execute("DROP TABLE IF EXISTS section CASCADE")
        cur.execute("DROP TABLE IF EXISTS chunk CASCADE")
        cur.execute("DROP TABLE IF EXISTS features CASCADE")

        cur.execute("CREATE TABLE author (author_id VARCHAR(20) PRIMARY KEY, author_name VARCHAR(1000))")
        cur.execute("CREATE TABLE paper (paper_id SERIAL PRIMARY KEY, paper_title VARCHAR(1000), syn_author_id SERIAL)")
        cur.execute(
            "CREATE TABLE writes_hidden (author_id VARCHAR(20), paper_id VARCHAR(20), author_num VARCHAR(20), CONSTRAINT writes_hidden_pkey PRIMARY KEY (author_id, paper_id))")
        cur.execute(
            "CREATE TABLE section (paper_id VARCHAR(20), section_id SERIAL, raw_text TEXT, novel_id VARCHAR(20), author_id VARCHAR(20), CONSTRAINT section_pkey PRIMARY KEY (paper_id,section_id))")
        cur.execute(
            "CREATE TABLE chunk (paper_id VARCHAR(20), start_sec VARCHAR(20), end_sec VARCHAR(20), chunk_id VARCHAR(20), CONSTRAINT chunk_pkey PRIMARY KEY (paper_id, chunk_id))")
        cur.execute(
            "CREATE TABLE features (paper_id VARCHAR(20), chunk_id VARCHAR(20), feature_id SERIAL, value VARCHAR(50), CONSTRAINT feature_pkey PRIMARY KEY (paper_id, chunk_id, feature_id))")

        con.commit()
        con.close()
        cur.close()

    def get_authors_id_200(self):
        con = psycopg2.connect("dbname ='%s' user='%s' host=/tmp/" % (getpass.getuser(), getpass.getuser()))
        cur = con.cursor()
        cur.execute("SELECT author_id FROM document_english GROUP BY author_id ORDER BY count(*) DESC")
        list_all = cur.fetchall()
        list_authors_id_200 = []
        for i in range(0, len(list_all)):
            list_authors_id_200.append(list_all[i][0])
        con.close()
        cur.close()
        return list_authors_id_200

    def get_authors_name(self, list_authors_id_200):
        con = psycopg2.connect("dbname ='%s' user='%s' host=/tmp" % (getpass.getuser(), getpass.getuser()))
        cur = con.cursor()
        list_authors_name = []
        for author in list_authors_id_200:
            cur.execute("SELECT author_name FROM author WHERE author_id = '%s'" % author)
            list_authors_name += cur.fetchall()[0]
        con.close()
        cur.close()
        return list_authors_name

    def get_num_paper_per_author(self, list_authors):
        list_temp = []
        for i in range(0, len(list_authors)):
            for j in range(0, len(list_authors[i][0:self.num_authors])):
                list_temp.append(list_authors[i][j])
        author_paper_dict = dict(Counter(list_temp))
        print(author_paper_dict)

    def get_authors(self, max_paper=15):
        con = psycopg2.connect("dbname ='%s' user='%s' host=/tmp/" % (getpass.getuser(), getpass.getuser()))
        cur = con.cursor()
        cur.execute("SELECT author_id, count(*) FROM document_english GROUP BY author_id ORDER BY count(*) DESC")
        list_all = cur.fetchall()
        list_top_200 = []
        list_top_200_max = []
        for i in range(len(list_all)):
            list_top_200.append(list_all[i][0])
            list_top_200_max.append(list_all[i][1])
        list_top_200_max = dict(zip(list_top_200, list_top_200_max))
        list_return = []
        dict_check = {k: 0 for k in list_top_200}

        j = 0
        print([value for key, value in list_top_200_max.iteritems()])
        while j < self.num_paper:
            list_return.append(list(np.random.permutation(list_top_200)[0:self.num_authors_list]))
            list_temp = []
            for x in range(0, len(list_return[j][0:self.num_authors])):
                dict_check[list_return[j][x]] += 1
                if dict_check[list_return[j][x]] > list_top_200_max[list_return[j][x]]:
                    try:
                        list_top_200.remove(list_return[j][x])
                        for y in range(0, x):
                            dict_check[list_return[j][y]] -= 1
                        list_return.remove(list_return[-1])
                        j -= 1
                        break
                    except ValueError:
                        break
            j += 1
        print([list_top_200_max[key] - value for key, value in dict_check.iteritems()])
        con.close()
        cur.close()
        return list_return

    def get_novel_list(self, author_id):
        con = psycopg2.connect("dbname ='%s' user='%s' host=/tmp/" % (getpass.getuser(), getpass.getuser()))
        cur = con.cursor()
        list_return = []
        cur.execute("SELECT doc_id FROM document_english WHERE author_id = '%s'" % (author_id))
        list_temp = cur.fetchall()
        for i in list_temp:
            list_return.append(i[0])
        con.close()
        cur.close()
        return list_return

    def get_raw_text(self, novel_id):
        con = psycopg2.connect("dbname ='%s' user='%s' host=/tmp/" % (getpass.getuser(), getpass.getuser()))
        cur = con.cursor()
        cur.execute("SELECT doc_content FROM document_english WHERE doc_id = '%s'" % (novel_id))
        raw_text = cur.fetchall()[0][0]
        return raw_text

    def get_novel_id(self, author_id, index=0):
        novel_id = self.get_novel_list(author_id=author_id)[index]
        return novel_id

    def get_paragraphs(self, tokens):
        paragraphs = [tokens[x:x + self.chunk_size] for x in xrange(0, self.token_size, self.sliding_window)]
        return paragraphs

    def save_authors_to_db(self, list_authors_id, list_authors_name):
        con = psycopg2.connect("dbname ='%s' user='%s' host=/tmp/" % (self.db_name.lower(), getpass.getuser()))
        cur = con.cursor()
        for i in range(0, len(list_authors_id)):
            cur.execute("INSERT INTO author VALUES(%s, %s)", [list_authors_id[i], list_authors_name[i]])
        con.commit()
        con.close()
        cur.close()

    def save_papers_to_db(self):
        con = psycopg2.connect("dbname ='%s' user='%s' host=/tmp/" % (self.db_name.lower(), getpass.getuser()))
        cur = con.cursor()
        for i in range(0, self.num_paper):
            name = "paper_number_%s" % (i + 1)
            cur.execute("INSERT INTO paper VALUES(%s, %s,%s)", [i + 1, name, i + 1])
        con.commit()
        con.close()
        cur.close()

    def save_writes_hidden_to_db(self, list_authors_id):
        con = psycopg2.connect("dbname ='%s' user='%s' host=/tmp/" % (self.db_name.lower(), getpass.getuser()))
        cur = con.cursor()
        for i in range(0, self.num_paper):
            for j in range(0, len(list_authors_id[i])):
                cur.execute("INSERT INTO writes_hidden VALUES(%s,%s,%s)", [int(list_authors_id[i][j]), i + 1, j + 1])
        con.commit()
        con.close()
        cur.close()

    def save_section_features_to_db(self, list_authors, list_authors_id_200):
        con = psycopg2.connect("dbname ='%s' user='%s' host=/tmp/" % (self.db_name.lower(), getpass.getuser()))
        cur = con.cursor()

        index = {}
        for m in list_authors_id_200:
            index[m] = 0

        num_section = 1
        chunk_id = 1

        for i in range(0, self.num_paper):
            tokens_sum = []

            for j in range(0, self.num_authors):
                novel_id = self.get_novel_id(author_id=list_authors[i][j], index=index[list_authors[i][j]])
                index[list_authors[i][j]] += 1

                raw_novel_text = self.get_raw_text(novel_id)
                tokens = nltk.word_tokenize(raw_novel_text.decode('utf-8'))
                tokens_sum += tokens[0:self.token_size / self.num_authors]

                cur.execute("INSERT INTO section VALUES(%s,%s,%s,%s,%s)", [i + 1, num_section,
                                                                           raw_novel_text, novel_id,
                                                                           int(list_authors[i][j])])
                num_section += 1

            paragraphs = self.get_paragraphs(tokens_sum)

            for x in range(0, len(paragraphs)):
                para = Paragraph("paper_id", para=paragraphs[x])
                stylo_list = []
                try:
                    stylo_list = para.get_stylo_list()
                except:
                    print('error')
                for y in range(0, 57):
                    feature_id = y + 1
                    try:
                        value = stylo_list[y]
                    except:
                        value = 0
                    cur.execute("INSERT INTO features VALUES (%s, %s, %s, %s) " % (i + 1, chunk_id, feature_id, value))
                chunk_id += 1

            con.commit()
            print("saved section no %s" % (i + 1))

        con.close()
        cur.close()


def parse_args():
    parser = argparse.ArgumentParser(description='Create a stylometry synthetic dataset.')
    parser.add_argument('--chunk_size', type=int, help='size of the chunk, number of token in the chunk')
    parser.add_argument('--token_size', type=int, help='size of the overall token')
    parser.add_argument('--num_authors', type=int, help='number of real authors')
    parser.add_argument('--num_authors_list', type=int, help='number of authors including generated one')
    parser.add_argument('--sliding_window', type=int, help='size of the sliding window')
    parser.add_argument('--num_paper', type=int, help='number of a paper to create database')
    """
  parser.add_argument('--chunk_size', type=int, default=600,
             help='size of the chunk, number of token in the chunk')
   parser.add_argument('--token_size', type=int, default=12000,
             help='size of the overall token')
   parser.add_argument('--num_authors', type=int, default=2,
             help='number of real authors')
   parser.add_argument('--num_authors_list', type=int, default=5,
             help='number of authors including generated one')
   parser.add_argument('--sliding_window', type=int, default=600,
             help='size of the sliding window')
   parser.add_argument('--num_paper', type=int, default=500,
             help='number of a paper to create database')

  """

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    syn_dataset = Syntactic(chunk_size=args.chunk_size, token_size=args.token_size,
                            num_authors=args.num_authors, num_authors_list=args.num_authors_list,
                            sliding_window=args.sliding_window, num_paper=args.num_paper)
    syn_dataset.create_db_table()

    list_authors_id_200 = syn_dataset.get_authors_id_200()
    list_authors_name = syn_dataset.get_authors_name(list_authors_id_200)
    syn_dataset.save_authors_to_db(list_authors_id_200, list_authors_name)

    list_authors = syn_dataset.get_authors()
    print(list_authors)
    print(len(list_authors))
    syn_dataset.save_papers_to_db()
    syn_dataset.save_writes_hidden_to_db(list_authors)
    syn_dataset.save_section_features_to_db(list_authors, list_authors_id_200)
