import psycopg2
import csv
#db_name = "syn_c500_t15000_a3_al5"
def get_syn(db_name,chunk_size,author_number,num_paper):
        con = psycopg2.connect("dbname ='%s' user='cpehk01' host=/tmp/" %(db_name))
        cur = con.cursor()
        print(db_name+" "+str(chunk_size)+" "+str(author_number))
        list_return = []

        chunk_num = 1
        for i in range(0,num_paper):   #number papers
                for j in range(0,chunk_size):    #number chunks per paper (token_size/chunk_size)
                        list_feature = []
                        chunk_per_fragment = 0
                        try:
                                chunk_per_fragment = (j/(chunk_size/author_number))
                        except ZeroDivisionError:
                                print('error: didided by 0') 
                        list_feature.append(str((chunk_per_fragment+1)+(author_number*i))) #first number is chunk per fragment, and the last number is number of authors (aka a2)
                        #list_feature.append(str(((j/10)+1)+(3*i)))
                        print(str(chunk_per_fragment))
                        print(str(chunk_per_fragment+1)+" "+str(author_number*i))
                        list_feature.append(str(i+1))
                        list_feature.append(str(chunk_num))
                        cur.execute("SELECT value FROM features WHERE paper_id = '%s' AND chunk_id = '%s'",[i+1,chunk_num])
                        temp = cur.fetchall()
                        for k in range(0,len(temp)):
                                list_feature.append(temp[k][0])
                        list_return.append(list_feature)
                        chunk_num += 1
        with open('./max/'+db_name+'.csv','w') as csvfile:
                write = csv.writer(csvfile,delimiter=',')
                for x in range(0,len(list_return)):
                        write.writerow(list_return[x])

#db_list = ['syn_eng_max_while_np1000_c1_t500_a2_al2_sw1']
for db_name in db_list:
        get_syn(db_name,int(db_name.split('_')[-4].split('t')[-1])/int(db_name.split('_')[-1].split('sw')[-1]),int(db_name.split('_')[-3].split('a')[-1]),int(db_name.split('_')[-6].split('np')[-1]))