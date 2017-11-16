import argparse

import requests
import re
import psycopg2
import lxml
from tqdm import tqdm
from os import walk
from bs4 import BeautifulSoup
import sys
import time

reload(sys)
sys.setdefaultencoding('utf8')


def remove_ref_tag(text):
    """
    remove a xml reference tag from the text
    :param text: text that contain xml ref tag
    :return: text with out a tag
    """
    if text is None:
        return ''
    text = re.sub('<xref.{1,70}</xref>\n', '', text.strip())
    return re.sub('<xref.{1,70}</xref>', '', text.strip())


def get_files_path_list(path):
    """
    get a list of path to each paper file
    :param path: directory that contain paper
    :return: list of full path to the paper
    """
    f = []
    for (dirpath, dirnames, filenames) in walk(path):
        f.extend([dirpath + '/' + s for s in filenames])
        break
    return f


def parse_xml(string):
    """
    parse a xml to BeautifulSoup object
    :param string: xml text
    :return: BeautifulSoup of the xml
    """
    return BeautifulSoup(string, "xml")


def get_file(file_path):
    """
    read the a file
    :param file_path: path to a file
    :return: string of xml
    """
    with open(file_path, 'r') as f:
        return f.read().encode('UTF-8')


def get_author(soup):
    """
    get all the author who wrote the paper
    :param soup: BeautifulSoup object
    :return: list of the author in the paper
    """
    names = []
    contrib = soup.find('contrib-group')
    author_count = 1
    if contrib is None:
        return None
    for name in contrib.findAll('name'):
        names.append((name.find('given-names').text, name.find('surname').text, author_count))
        author_count += 1
    return names


def get_raw_text(root):
    """
    get a raw text from a paper
    :param root: BeautifulSoup object
    :return: raw text of the paper
    """
    raw_text = ""
    body = root.find('body')
    if body is None:
        return None
    for sec in body.findAll('sec'):
        if "Cite this paper" in sec.find('title').text:
            continue
        raw_text += sec.find('title').text + '\n'
        for p in sec.findAll('p'):
            print(p.text)
            raw_text += p.text + '\n'
    return raw_text.encode("utf-8")


def get_title(root):
    """
    get the title of the paper
    :param root: BeautifulSoup object
    :return: paper title
    """
    if root.find("article-title") is None:
        return None
    return root.find("article-title").text


def get_categories(root):
    """
    get categories of the paper
    :param root: BeautifulSoup object
    :return: list of categories that the paper is in
    """
    categories = []
    article_categories = root.find("article-categories")
    if article_categories is None:
        return None
    for categore in article_categories.findAll('subject'):
        categories.append(categore.text)
    return categories


def get_con_cur(db_name):
    """
    connect to a database
    :param db_name: name of the database
    :return: con: database connection
            cur: cursor of the connection
    """
    con = psycopg2.connect("dbname ='%s' user='cpehk01' host=/tmp/" % (db_name))
    return con, con.cursor()


def drop_all_table(db_name):
    """
    drop all the table in database
    :param db_name: name of the database that wanted to be drop
    """
    con = psycopg2.connect("dbname='%s' user='cpehk01' host=/tmp/" % (db_name))
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS author, paper, author_paper, paper_category")
    con.commit()
    con.close()


def create_database(db_name):
    """
    create a new database
    :param db_name: database name
    """
    con = psycopg2.connect("dbname='%s' user='cpehk01' host=/tmp/" % (db_name))
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS author (author_id SERIAL PRIMARY KEY, name VARCHAR(200), surname VARCHAR(200))")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS paper (paper_id SERIAL PRIMARY KEY,scirp_id INTEGER, paper_title VARCHAR(1000),raw_text TEXT)")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS author_paper (author_id SERIAL REFERENCES author(author_id) ON UPDATE CASCADE, paper_id SERIAL REFERENCES paper(paper_id) ON UPDATE CASCADE, author_num INTEGER, CONSTRAINT author_paper_pk PRIMARY KEY (paper_id, author_id,author_num))")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS paper_category (paper_id SERIAL REFERENCES paper(paper_id) ON UPDATE CASCADE, category VARCHAR(200) , CONSTRAINT paper_category_pk PRIMARY KEY (paper_id, category))")
    con.commit()
    con.close()


def insert_author(cur, name, surname):
    """
    insert a author into a database
    :param cur: cursor
    :param name: name of the author
    :param surname: surname of the author
    """
    cur.execute(
        "INSERT INTO author (name,surname) SELECT %s,%s WHERE NOT EXISTS (SELECT name,surname FROM author WHERE name =%s AND surname = %s )",
        (name, surname, name, surname))


def get_author_id(cur, name, surname):
    cur.execute("SELECT author_id FROM author WHERE name = %s AND surname = %s", (name, surname))


def get_paper_id(cur, scirp_id):
    cur.execute("SELECT paper_id FROM paper WHERE scirp_id = %s", (scirp_id,))


def insert_paper(cur, paper_id, paper_title, raw_text):
    cur.execute("INSERT INTO paper (scirp_id,paper_title, raw_text) VALUES(%s,%s,%s)",
                (paper_id, paper_title, raw_text))


def insert_author_paper(cur, author_id, paper_id, author_num):
    cur.execute("INSERT INTO author_paper (author_id,paper_id,author_num) VALUES(%s,%s,%s)",
                (author_id, paper_id, author_num))


def insert_paper_category(cur, paper_id, categorie):
    cur.execute("INSERT INTO paper_category (paper_id,category) VALUES(%s,%s)", (paper_id, categorie))


def is_xml(string):
    return "xmlns:mml" in string

def execute(con, cur, title, authors, raw_text, categories, scirp_id):
    author_ids = []
    for author in authors:
        insert_author(cur, author[0], author[1])
        get_author_id(cur, author[0], author[1])
        author_id = cur.fetchall()[0][0]
        author_ids.append(author_id)

    insert_paper(cur, scirp_id, title, raw_text)
    get_paper_id(cur, scirp_id)
    paper_id = cur.fetchall()[0][0]
    for i, author_id in enumerate(author_ids):
        insert_author_paper(cur, author_id, paper_id, authors[i][2])
    for category in categories:
        insert_paper_category(cur, paper_id, category)


def parser_args():
    parser = argparse.ArgumentParser(description='Get a stylometry synthetic data.')
    parser.add_argument('--path', type=str, help='directory path the of the paper')
    parser.add_argument('--db_name', type=str, nargs='*', help="database name that want to get")
    return parser.parse_args()


def run():
    arg = parser_args()
    db_name = arg.db_name

    # con, cur = get_con_cur(db_name)
    # drop_all_table(db_name)
    # create_database(db_name)

    for file_path in tqdm(get_files_path_list(arg.path)):
        file = get_file(file_path)
        if is_xml(file):
            tqdm.write(file_path)
            file = remove_ref_tag(file)
            xml = parse_xml(file)
            # tqdm.write(str(get_author(xml)))
            # tqdm.write(str(get_raw_text(xml)))
            # tqdm.write(str(get_categories(xml)))
            title = get_title(xml)
            authors = get_author(xml)
            raw_text = get_raw_text(xml)
            categories = get_categories(xml)
            #if title and authors and raw_text and categories:
                # tqdm.write(str(title)+' '+str(authors)+' '+str(len(raw_text))+' '+str(categories)):

                # write to database
                # execute(con, cur, title, authors, raw_text, categories, file_path[-9:-4])
                #con.commit()
    # con.close()


if __name__ == "__main__":
    start = time.time()
    run()
    end = time.time()
    print("time:")
    print(end - start)
