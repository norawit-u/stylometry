import psycopg2
import csv
#d b_name = "syn_c500_t15000_a3_al5"

def get_parameter(name, position, word):
  return int(db_name.split('_')[position].split(word)[-1])

def get_syn(db_name, paper_num, chunk_size, author_number):
  # create connection to database
  con = psycopg2.connect("dbname ='%s' user='stylometry'" %(db_name))
  # create cursor
  cur = con.cursor()
  print(db_name+" "+str(chunk_size)+" "+str(author_number))
  list_return = [] # list of the feature,
  chunk_num = 1
  for i in range(0, paper_num):   #number papers
    for j in range(0, chunk_size):  #number chunks per paper (token_size/chunk_size)
      # section_id, fragment_id, chunck_id, author_id, 57 features ...
      list_feature = [] # list of this row
      list_feature.append(str(((j/(chunk_size/author_number))+1)+(author_number*i))) #first number is chunk per fragment, and the last number is number of authors (aka a2)
      #list_feature.append(str(((j/10)+1)+(3*i)))
      print(str(j/10+1))
      print(str((j/(chunk_size / author_number))+1) + " " + str(author_number*i))
      list_feature.append(str(i+1))
      list_feature.append(str(chunk_num))
      cur.execute("SELECT value FROM features WHERE paper_id = '%s' AND chunk_id = '%s'", [i+1, chunk_num])
      temp = cur.fetchall()
      for k in range(0, len(temp)):
        list_feature.append(temp[k][0])
      list_return.append(list_feature)
      chunk_num += 1
  with open('./same_number_of_author/'+db_name+'.csv', 'w') as csvfile:
    write = csv.writer(csvfile, delimiter=',')
    for x in range(0, len(list_return)):
      write.writerow(list_return[x])

db_list = ["syn_eng_np100_c600_t12000_a10_al10_sw600"]
for db_name in db_list:
  get_syn(db_name, get_parameter(db_name, -6,'np'), 12000/get_parameter(db_name, -1, "sw"), get_parameter(db_name, -3,"a"))