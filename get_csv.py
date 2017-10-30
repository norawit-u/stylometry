import csv
import getpass
import psycopg2

USER = getpass.getuser() # database username

#db_name = "syn_c500_t15000_a3_al5"
db_name = "syn_eng_c600_t12000_a2_al5_sw600"
con = psycopg2.connect("dbname ='%s' user='%s' host=local" %(db_name,USER))
cur = con.cursor()

"""
    fragment_size: number of chuncks in a fragment
    offset: number of chucks between n and n+1 fragment
    chunk_size: number of chunks
"""
def is_fragmentable(fragment_size, offset, chunk_size):
    return ((chunk_size - fragment_size) / offset) % 1 == 0

def get_fragments(fragment_size, offset, chunk_size):
    if is_fragmentable(fragment_size, offset, chunk_size):
            return [tokens[x:x + fragment_size] for x in xrange(0, len(chunk_size), offset)]

def get_num_fragment(fragment_size, offset, chunk_size):
    return (chunk_size - fragment_size) / offset +1

def save_to_csv(list_return):
    with open('./syn_a2_sw600.csv', 'w') as csvfile:
        write = csv.writer(csvfile, delimiter=',')
        for x in range(0, len(list_return)):
            write.writerow(list_return[x])

def get_features(num_paper, fragment_size, offset, num_fragment):
    list_return = []
    fragment_count = 1
    chunk_count = 1 + fragment_size
    chunk_number = 1
    for i in range(0, num_paper):
        for j in range(fragment_count, fragment_count+num_fragment):
            chunk_count -= fragment_size
            for k in range(chunk_count, chunk_count+fragment_size):
                list_feature = []
                list_feature.append(str(j)) # fragment id
                list_feature.append(str(i+1)) # paper id 
                list_feature.append(str(chunk_number)) # chunk id
                cur.execute("SELECT value FROM features WHERE paper_id = '%s' AND chunk_id = '%s'", [i+1, k])
                temp = cur.fetchall()
                for l in range(0, len(temp)):
                    list_feature.append(temp[l][0])
                list_return.append(list_feature)
                chunk_count += 1
                chunk_number += 1
            chunk_count += offset
        chunk_count += fragment_size - offset
        fragment_count += num_fragment

if __name__ == '__main__':
    fragment_size = 10
    offset = 2
    chunk_size = 20
    num_paper = 500
    if is_fragmentable(fragment_size, offset, chunk_size):
        num_fragment = get_num_fragment(fragment_size, offset, chunk_size)
        list_return = get_features(num_paper, fragment_size, offset, num_fragment)
        save_to_csv(list_return)


 

