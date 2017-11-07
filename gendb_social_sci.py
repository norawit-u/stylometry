import argparse

import numpy as np
import psycopg2
from paragraph import Paragraph
import nltk
import getpass
import sys
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from collections import Counter

from six.moves import xrange

class Syntactic:
    def __init__(self, chunk_size, token_size, num_authors, num_authors_list, sliding_window, num_paper):
        self.chunk_size = chunk_size
        self.token_size = token_size
        self.num_authors = num_authors
        self.num_authors_list = num_authors_list
        self.sliding_window = sliding_window
        self.copus_db_name = 'social_sci_paper'
        self.db_name = "syn_social_c%s_t%s_a%s_al%s_sw%s" % (
            chunk_size, token_size, num_authors, num_authors_list, sliding_window)
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
        cur.execute("CREATE TABLE paper (paper_id SERIAL PRIMARY KEY, paper_title VARCHAR(1000),syn_author_id SERIAL)")
        cur.execute(
            "CREATE TABLE writes_hidden (author_id VARCHAR(20), paper_id VARCHAR(20),author_num VARCHAR(20), CONSTRAINT writes_hidden_pkey PRIMARY KEY (author_id, paper_id))")
        cur.execute(
            "CREATE TABLE section (paper_id VARCHAR(20), section_id SERIAL, raw_text TEXT, novel_id VARCHAR(20), author_id VARCHAR(20), CONSTRAINT section_pkey PRIMARY KEY (paper_id,section_id))")
        cur.execute(
            "CREATE TABLE chunk (paper_id VARCHAR(20), start_sec VARCHAR(20), end_sec VARCHAR(20), chunk_id VARCHAR(20), CONSTRAINT chunk_pkey PRIMARY KEY (paper_id, chunk_id))")
        cur.execute(
            "CREATE TABLE features (paper_id VARCHAR(20), chunk_id VARCHAR(20), feature_id SERIAL, value VARCHAR(50),CONSTRAINT feature_pkey PRIMARY KEY (paper_id, chunk_id, feature_id))")

        con.commit()
        con.close()
        cur.close()

    def get_authors_name(self, list_authors_id):
        con = psycopg2.connect("dbname ='%s' user='%s' host=/tmp/" % (self.copus_db_name, getpass.getuser()))
        cur = con.cursor()
        authors_names = []
        for author in list_authors_id:
            cur.execute("SELECT name,surname FROM author WHERE author_id = '%s'" % author)
            temp = cur.fetchall()
            authors_names += str(temp[0]).decode('utf8').strip() + ' ' + str(temp[0]).decode('utf8').strip()
        con.close()
        cur.close()
        return authors_names

    def get_novel_list(self, author_id):
        con = psycopg2.connect("dbname ='%s' user='%s' host=/tmp/" % (self.copus_db_name, getpass.getuser()))
        cur = con.cursor()
        list_return = []
        cur.execute("SELECT paper_id FROM author_paper WHERE author_id = '%s'" % (author_id))
        list_temp = cur.fetchall()
        for i in list_temp:
            list_return.append(i[0])
        con.close()
        cur.close()
        return list_return

    def get_raw_text(self, novel_id):
        con = psycopg2.connect("dbname ='%s' user='%s' host=/tmp/" % (self.copus_db_name, getpass.getuser()))
        cur = con.cursor()
        cur.execute("SELECT raw_text FROM paper WHERE paper_id = '%s'" % (novel_id))
        raw_text = cur.fetchall()[0][0].strip()
        return raw_text

    def get_novel_id(self, author_id, index=0):
        print(author_id,index)
        novel_id = self.get_novel_list(author_id=author_id)[index]
        return novel_id

    def get_paragraphs(self, tokens):
        paragraphs = [tokens[x:x + self.chunk_size] for x in xrange(0, self.token_size, self.sliding_window)]
        return paragraphs

    def save_authors_to_db(self, list_authors_id, list_authors_name):
        con = psycopg2.connect("dbname ='%s' user='%s' host=/tmp/" % (self.db_name.lower(), getpass.getuser()))
        cur = con.cursor()
        for i in range(0, len(list_authors_id)):
            cur.execute("INSERT INTO author VALUES(%s, %s)", [str(list_authors_id[i]), str(list_authors_name[i])])
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

                cur.execute("INSERT INTO section VALUES(%s,%s,%s,%s,%s)",
                            [i + 1, num_section, raw_novel_text, novel_id, list_authors[i][j]])
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

    def get_paper_ids(self):
        con = psycopg2.connect("dbname ='%s' user='%s' host=/tmp/" % (self.copus_db_name, getpass.getuser()))
        cur = con.cursor()
        papers_id = []
        cur.execute("select paper_id, count(*) from author_paper group by paper_id having count(*) = %s" % self.num_authors)
        list_temp = cur.fetchall()
        for i in list_temp:
            papers_id.append(i[0])
        con.close()
        cur.close()
        return papers_id

    def get_authors(self, paper_ids):
        con = psycopg2.connect("dbname ='%s' user='%s' host=/tmp/" % (self.copus_db_name, getpass.getuser()))
        cur = con.cursor()
        authors = []
        for paper_id in paper_ids:
            cur.execute("select author_id from author_paper where paper_id = '%s'" % paper_id)
            list_temp = cur.fetchall()
            authors_id = []
            for i in list_temp:
                authors_id.append(i[0])
            authors.append(authors_id)
        con.close()
        cur.close()
        return authors

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

    paper_ids = syn_dataset.get_paper_ids()
    author_ids = syn_dataset.get_authors(paper_ids)
    all_author_ids = np.concatenate(author_ids)
    list_authors_name = syn_dataset.get_authors_name(all_author_ids)
    syn_dataset.save_authors_to_db(all_author_ids, list_authors_name)

    syn_dataset.save_papers_to_db()
    syn_dataset.save_writes_hidden_to_db(author_ids)
    syn_dataset.save_section_features_to_db(author_ids, all_author_ids)
