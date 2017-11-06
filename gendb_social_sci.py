import numpy as np
import psycopg2
from paragraph import Paragraph
import nltk
import getpass
import sys
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from collections import Counter


class Syntactic:
    def __init__(self, chunk_size, token_size, num_authors, num_authors_list, sliding_window, num_paper):
        self.chunk_size = chunk_size
        self.token_size = token_size
        self.num_authors = num_authors
        self.num_authors_list = num_authors_list
        self.sliding_window = sliding_window
        self.db_name = "syn_social_c%s_t%s_a%s_al%s_sw%s" % (
        chunk_size, token_size, num_authors, num_authors_list, sliding_window)
        self.num_paper = num_paper

    def create_db_table(self):
        con = psycopg2.connect("dbname ='postgres' user='%s' host='/tmp/'" % (getpass.getuser()))
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute("DROP DATABASE IF EXISTS " + self.db_name)
        cur.execute("CREATE DATABASE " + self.db_name)
        con.close()
        cur.close()
        con = psycopg2.connect("dbname ='%s' user='%s' host='/tmp/'" % (self.db_name.lower(), getpass.getuser()))
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
            "CREATE TABLE chunk (paper_id VARCHAR(20), start_sec VARCHAR(20), end_sec VARCHAR(20), chunk_id VARCHAR(20), CONSTRAINT chunk_pkey PRIMARY KEY (paper_id,chunk_id))")
        cur.execute(
            "CREATE TABLE features (paper_id VARCHAR(20), chunk_id VARCHAR(20), feature_id SERIAL, value VARCHAR(50),CONSTRAINT feature_pkey PRIMARY KEY (paper_id,chunk_id,feature_id))")

        con.commit()
        con.close()
        cur.close()

    def get_authors_id_200(self):
        con = psycopg2.connect("dbname ='%s' user='%s' host='/tmp/'" % (getpass.getuser(), getpass.getuser()))
        cur = con.cursor()
        cur.execute("SELECT author_id FROM author_paper GROUP BY author_id ORDER BY count(*) DESC")
        list_all = cur.fetchall()
        list_authors_id_200 = []
        for i in range(0, 200):
            list_authors_id_200.append(list_all[i][0])
        con.close()
        cur.close()
        return list_authors_id_200

    def get_authors_name(self, list_authors_id_200):
        con = psycopg2.connect("dbname ='%s' user='%s' host='/tmp/'" % (getpass.getuser(), getpass.getuser()) )
        cur = con.cursor()
        authors_names = []
        for author in list_authors_id_200:
            cur.execute("SELECT name,lastname FROM author WHERE author_id = '%s'" % author)
            temp = cur.fetchall()
            authors_names += temp[0][0] + temp[0][1]
        con.close()
        cur.close()
        return authors_names

    def get_num_paper_per_author(self, list_authors):
        list_temp = []
        for i in rage(0, len(list_authors)):
            for j in range(0, len(list_authors[i][0:self.num_authors])):
                list_temp.append(list_authors[i][j])
        author_paper_dict = dict(Counter(list_temp))
        print(author_paper_dict)

    def get_authors(self, max_paper=15):
        con = psycopg2.connect("dbname ='%s' user='%s' host='/tmp/'" % (getpass.getuser(), getpass.getuser()))
        cur = con.cursor()
        cur.execute("SELECT author_id FROM author_paper GROUP BY author_id ORDER BY count(*) DESC")
        list_all = cur.fetchall()
        list_top_200 = []

        for i in range(0, 200):
            list_top_200.append(list_all[i][0])

        list_return = []
        dict_check = {k: 0 for k in list_top_200}

        for j in range(0, self.num_paper):
            list_return.append(list(np.random.permutation(list_top_200)[0:self.num_authors_list]))
            list_temp = []
            for x in range(0, len(list_return[j][0:self.num_authors])):
                dict_check[list_return[j][x]] += 1
                if dict_check[list_return[j][x]] > max_paper:
                    try:
                        list_top_200.remove(list_return[j][x])
                    except:
                        continue

        con.close()
        cur.close()
        return list_return

    def get_novel_list(self, author_id):
        con = psycopg2.connect("dbname ='%s' user='%s' host='/tmp/'" % (getpass.getuser(), getpass.getuser()))
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
        con = psycopg2.connect("dbname ='%s' user='%s' host='/tmp/'" % (getpass.getuser(), getpass.getuser()))
        cur = con.cursor()
        cur.execute("SELECT raw_text FROM paper WHERE paper_id = '%s'" % (novel_id))
        raw_text = cur.fetchall()[0][0]
        return raw_text

    def get_novel_id(self, author_id, index=0):
        novel_id = self.get_novel_list(author_id=author_id)[index]
        return novel_id

    def get_paragraphs(self, tokens):
        paragraphs = [tokens[x:x + self.chunk_size] for x in xrange(0, self.token_size, self.sliding_window)]
        return paragraphs

    def save_authors_to_db(self, list_authors_id, list_authors_name):
        con = psycopg2.connect("dbname ='%s' user='%s' host='/tmp/'" % (self.db_name.lower(), getpass.getuser()))
        cur = con.cursor()
        for i in range(0, len(list_authors_id)):
            cur.execute("INSERT INTO author VALUES(%s, %s)", [list_authors_id[i], list_authors_name[i]])
        con.commit()
        con.close()
        cur.close()

    def save_papers_to_db(self):
        con = psycopg2.connect("dbname ='%s' user='%s' host='/tmp/'" % (self.db_name.lower(), getpass.getuser()))
        cur = con.cursor()
        for i in range(0, self.num_paper):
            name = "paper_number_%s" % (i + 1)
            cur.execute("INSERT INTO paper VALUES(%s, %s,%s)", [i + 1, name, i + 1])
        con.commit()
        con.close()
        cur.close()

    def save_writes_hidden_to_db(self, list_authors_id):
        con = psycopg2.connect("dbname ='%s' user='%s' host='/tmp/'" % (self.db_name.lower(), getpass.getuser()))
        cur = con.cursor()
        for i in range(0, self.num_paper):
            for j in range(0, len(list_authors_id[i])):
                cur.execute("INSERT INTO writes_hidden VALUES(%s,%s,%s)", [list_authors_id[i][j], i + 1, j + 1])
        con.commit()
        con.close()
        cur.close()

    def save_section_features_to_db(self, list_authors, list_authors_id_200):
        con = psycopg2.connect("dbname ='%s' user='%s' host='/tmp/'" % (self.db_name.lower(), getpass.getuser()))
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
                for y in range(0, 57):
                    feature_id = y + 1
                    try:
                        value = para.get_stylo_list()[y]
                    except:
                        value = 0
                    cur.execute("INSERT INTO features VALUES (%s, %s, %s, %s) " % (i + 1, chunk_id, feature_id, value))
                chunk_id += 1

            con.commit()
            print("saved section no %s" % (i + 1))

        con.close()
        cur.close()


if __name__ == "__main__":
    syn_dataset = Syntactic(chunk_size=1000, token_size=12000, num_authors=2, num_authors_list=5, sliding_window=600,
                            num_paper=500)
    syn_dataset.create_db_table()

    list_authors_id_200 = syn_dataset.get_authors_id_200()
    list_authors_name = syn_dataset.get_authors_name(list_authors_id_200)
    syn_dataset.save_authors_to_db(list_authors_id_200, list_authors_name)

    list_authors = syn_dataset.get_authors()

    syn_dataset.save_papers_to_db()
    syn_dataset.save_writes_hidden_to_db(list_authors)
    syn_dataset.save_section_features_to_db(list_authors, list_authors_id_200)
    syn_dataset.save_section_features_to_db(list_authors, list_authors_id_200)
