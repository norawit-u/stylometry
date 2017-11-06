import requests
import re
import psycopg2
import lxml
from os import walk
from bs4 import BeautifulSoup
import sys

reload(sys)
sys.setdefaultencoding('utf8')
"""
NOT USED
"""

def remove_ref_tag(text):
        if text is None:
                return ''
        text = re.sub('<xref.{1,70}</xref>\n', '', text.strip())
        return re.sub('<xref.{1,70}</xref>', '', text.strip())

def get_file_path_list(path):
        f = []
        for (dirpath, dirnames, filenames) in walk(path):
                f.extend([dirpath+'\\' + s for s in filenames])
                break
        return f

def parse_xml(string):
        return BeautifulSoup(string, "xml")

def get_file(file_path):
        with open(file_path,'r') as f:
                return f.read().encode('UTF-8')
        f.close()

def get_author(soup):
        names =[]
        contrib = soup.find('contrib-group')
        author_count = 1
        for name in contrib.findAll('name'):
                names.append((name.find('given-names').text,name.find('surname').text,author_count))
                author_count+=1
        return names

def get_raw_text(root):
        raw_text =""
        body = root.find('body')
        for sec in body.findAll('sec'):
                if "Cite this paper" in sec.find('title').text:
                        continue
                raw_text += sec.find('title').text+'\n'
                for p in sec.findAll('p'):
                                raw_text+=p.text+'\n'
        return raw_text.encode("utf-8")

def get_title(root):
        return root.find("article-title").text()

def get_categories(root):
        categories = []
        for categore in root.find("article-categories").findAll('subject'):
                categories.append(categore.text)
        return categories

def get_pg_cursor():
        con = psycopg2.connect("dbname ='%s' user='stylometry'" %(db_name))
        return con.cursor()

def create_database():
    con = psycopg2.connect("dbname='cpedb' user='cpehk'")
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS author (author_id SERIAL PRIMARY KEY, name VARCHAR(200), surname VARCHAR(200))")
    cur.execute("CREATE TABLE IF NOT EXISTS paper (paper_id SERIAL PRIMARY KEY,scirp_id INTEGER, paper_title VARCHAR(1000)),raw_text TEXT")
    cur.execute("CREATE TABLE IF NOT EXISTS author_paper (author_id SERIAL REFERENCES author(author_id) ON UPDATE CASCADE, paper_id SERIAL REFERENCES paper(paper_id) ON UPDATE CASCADE, CONSTRAINT author_paper PRIMARY KEY (paper_id, author_id))")
    cur.execute("CREATE TABLE IF NOT EXISTS paper_category (paper_id SERIAL REFERENCES paper(paper_id) ON UPDATE CASCADE, category VARCHAR(200) , CONSTRAINT paper_category PRIMARY KEY (paper_id, cagegory))")
    con.commit()

def insert_authro(author):
        insert_queue = "INSERT INTO author (name,surname) VALUEs(%s,%s) WHERE NOT EXISTS (SELECT name,surname FROM author WHERE name ='%s' AND surname = '%s' )"
        cur.execute(insert_queue,author,author)

def run():
        for file_path in get_file_path_list("C:\\Users\\wit54\\Desktop\\lsh\\paper"):
                file = remove_ref_tag(get_file(file_path))
                xml = parse_xml(file)
                print( get_author(xml))
                    print(get_raw_text(xml))
                print(get_categories(xml))



if __name__ == "__main__":
        run()