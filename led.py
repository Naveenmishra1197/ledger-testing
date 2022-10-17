from ast import Pass
from curses.ascii import isalpha
from google.cloud import storage
from fcntl import F_GETLK64
import os
from tracemalloc import Snapshot

#storage_client = storage.Client()
#blobs = storage_client.list_blobs('mainnet-beta-ledger-asia-sg1', prefix='100501916/', delimiter='bounds.txt/')

os.system('pip3 install gspread')
os.system('pip3 install oauth2client')

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# define the scope

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
# add credentials to the account

creds = ServiceAccountCredentials.from_json_keyfile_name('solana-devnet-507716491791.json', scope)
# authorize the clientsheet

client = gspread.authorize(creds)
# get the instance of the Spreadsheet

sheet = client.open('Untitled spreadsheet')
print('sheet id is : ',sheet.id)

if not os.path.exists('list_of_slots_sheet.txt'):
    fh = open('list_of_slots_sheet.txt','a')
    fh.close()

# fh = open('list_of_slots.txt','r')
# slots_output = fh.readlines()
# fh.close()
sheet_instance = sheet.get_worksheet(0)
count_row = sheet_instance.row_count
print(count_row)
val_B_range = 'B1:B'+str(count_row)
val_B = sheet_instance.get_values(val_B_range)
# print('val_B is : ',str(val_B))

len_val_B = len(val_B)
print('length of filled rows is ',len_val_B)
fhand = open('list_of_slots_sheet.txt','w')
for i in range(len_val_B):
    fhand.write(val_B[i][0])
    fhand.write('\n')


fhand.close()



#Change bucket name 
bucket_name=["mainnet-beta-ledger-us-ny5","mainnet-beta-ledger-europe-fr2","mainnet-beta-ledger-asia-sg1"]
count = 0

f2=[]
f3=[]
f5=[]
f6=[]
f7=[]
f8=[]
f9=[]
#f2.append(rocksdb_path) f3.append(rocksdb compression) f5.append(has_bounds) f6.append(slot_count) f7.append(start_slot) f8.append(end_slot)
for bucket in bucket_name :
  filename = 'detail_of_spreadsheet'+str(count)+'.csv'
  if os.path.exists(filename):
    os.remove(filename)
    print('removing old {} file '.format(filename))

  details_for_spreadsheet = 'slot_name,bucket_,has_bounds,slot_count,start_slot,end_slot,gap,rocksdb_path,rock_size,rocksdb_compression,snapshot_url\n'
  fhand = open(filename, 'a' )
  fhand.write(details_for_spreadsheet)
  fhand.close() 
  count += 1
  gap = ''
  def bounds_content(bucket,direc):
    try:
       file=str(direc)+"/bounds.txt"
       gcs_url = 'https://%(bucket)s.storage.googleapis.com/%(file)s' % {'bucket':bucket, 'file':file}
       #print(gcs_url)
       fhandle=os.popen('curl '+gcs_url)
       output_url=fhandle.readlines()
       fhandle.close()
    #    print('output_url is : ',output_url)
       slot_split= ((output_url[0].strip('\n').strip(' ')).split(' '))[6:10:2]
    #    print('slot_split is: ', slot_split)
       #print(slot_split,slot_split[1],slot_split[0],str(int(slot_split[1])-int(slot_split[0])))
    #    print('checking the bounds content from try')
       #print(slot_split[1],slot_split[0])
       start_slot1 = slot_split[0]
       end_slot1 = slot_split[1]
    #    print('start {}'.format(start_slot1),'and the end slot is {}'.format(end_slot1))
       if not slot_split[0].isalpha() and not slot_split[1].isalpha():
           print(slot_split[1],slot_split[0])
       else:
           slot_split[0] = 0
           slot_split[1] = 0
       return [slot_split[1],slot_split[0]];

    except:
        print('checking the bounds content from except')
        try:
            print('inside try, slot split')
            slot_split= ((output_url[0].strip('\n')).split(' '))[5:9:2]
            if not slot_split[0].isalpha() and not slot_split[1].isalpha():
                print(slot_split[1],slot_split[0])
            else:
                slot_split[0] = '0'
                slot_split[1] = '0'
            #print(slot_split,slot_split[1],slot_split[0])
            return [slot_split[1],slot_split[0]];
        except:
            print('except within except block')
            slot_split[0] = '0'
            slot_split[1] = '0'
            pass


  def list_blobs(bucket,filename): 
    """Lists all the blobs in the bucket."""
    fhandle1 = os.popen('gsutil ls gs://'+ str(bucket))
    output1 = fhandle1.read()
    fhandle1.close()
    fh = open('list_of_slots_bucket.txt','a')
    fh.write(output1)
    fh.close()
    output1 = output1.strip('\n').split('\n')

    # details_for_spreadsheet = 'slot_name,bucket_,has_bounds,slot_count,start_slot,end_slot,gap,rocksdb_path,rock_size,rocksdb_compression,snapshot_url\n'
    # fhand = open('detail_of_spreadsheet.csv', 'a' )
    # fhand.write(details_for_spreadsheet)
    # fhand.close() 

    trace_1st_bucket = 0
    gap = ''

    for slot_ in output1:
        print('slot_ is :',slot_)
        has_bounds= False
        snapshot_url = ''
        rocksdb_path = ''
        rock_size = ''
        rocksdb_compression = False
        slots_ = (slot_.strip('\n').split('/'))
        print('slots_ is :',slots_)
        #print( dir_[-2])
        slot_name = slots_[-2]
        fhand = open('list_of_slots_sheet.txt')
        out = fhand.read()
        fhand.close()
        out_list = out.strip('\n').split('\n')
        #print('out_list is : ', out_list)

        # for i in range(len(out)):
        if slot_name in out_list:
            print('{} already exist in the spreadsheet'.format(slot_name))
            continue
        else:
            if '.temp' not in slot_name and 'mainnet' not in slot_name:
                print('slot name is : ',slot_name)
            else:
                #print('temp found')
                continue
            trace_1st_bucket += 1

            print('bucket is : ',bucket)
            fhandle2 = os.popen('gsutil ls gs://'+ str(bucket)+'/'+str(slot_name))
            output2 = fhandle2.readlines()
            fhandle2.close()
            for file_ in output2:
                #print('file_ is : ',file_)
                file_name=(file_.strip('\n').strip(' ').split('/')[-1])
                #print('file name is :',file_name)

                #to find if bounds file exist then extract its data
                if ("bounds" in file_name):
                    bucket_=bucket
                    #print('bucket is : ',bucket_)
                    #print('check 24')
                    # if trace_1st_bucket == 1:
                    #     prev_end_slot = 0
                    # else:
                    #     prev_end_slot = end_slot
                    has_bounds= True
                    print('has bounds: True')
                    end_slot,start_slot = bounds_content(bucket_,slot_name)
                    # print('end_slot is {}'.format(end_slot),'start_slot is {}'.format(start_slot))
                    slot_count = str(int(end_slot) - int(start_slot))
                    print("slot count: {} \n".format(slot_count),"start slot: {} \n".format(start_slot),"end slot: {}".format(end_slot))
                    # if (int(prev_end_slot) - int(start_slot)) > 0:
                    #     gap = int(prev_end_slot) - int(start_slot)
                    # else :
                    #     gap = ''
                #else:
                #    print('has_bounds: False')
                #    print('end slot: {}'.format(end_slot),'start slot: {}'.format(start_slot),'slot count: {}'.format(int(end_slot)-int(start_slot)))
                if ("rocksdb" in file_name):
                    rocksdb_path = 'gs://' + str(bucket) + '/' + str(slot_name) + '/' + str(file_name)
                    print(rocksdb_path)
                if ("snapshot" in file_name):
                    if ("hourly" not in file_name):
                        snapshot_url = 'gs://' + str(bucket) + '/' + str(slot_name) + '/' + str(file_name)
                        print("snapshot_url: ",snapshot_url)

                try:
                    #to find if the rocsdb_tar file is of size > or < 10GB
                    #print('check 26')
                    
                    #to find if bounds file exist then extract its data
                    if "rocksdb.tar." in file_name:
                        #print('check 27')
                        #print(blob.name,"Size: {} GB".format(((blob.size)/1024)/1024/1024))
                        fhandle3 = os.popen('gsutil ls -L ' + str(rocksdb_path))
                        output3 = fhandle3.readlines()
                        fhandle3.close()
                        #print(output3)
                        output3a = output3[5].strip('\n').strip(' ').split(' ')
                        rock_size_bytes = int(output3a[-1])
                        print('rock_size_bytes:', str(rock_size_bytes))
                        rock_size= int(((rock_size_bytes)/1024)/1024/1024)
                        if int(rock_size) > 10 : 
                            rocksdb_compression = True
                            print('rocksdb_compression: ',rocksdb_compression)
                        #     print('check 28')
                        #     #print('if statement is running')
                        #     f3.append('True')


                            #print("rocksdb compression: True")
                        else :
                        #     #print('else statement is running')
                        #     print('check 9')
                        #     f3.append('False')
                            print("rocksdb compression: False")
                        # else:
                        #   print('rockdbpath: not found')
                except:
                    print('check 30')
                    # f3.append('Not Found')
                    #print("rocksdb compression: not found")
            details_for_spreadsheet = str(slot_name) + ',' + str(bucket_) + ',' +  str(has_bounds) + ',' +  str(slot_count) + ',' +  str(start_slot) + ',' +  str(end_slot) + ',' + str(gap)+ ',' + str(rocksdb_path) + ',' + str(rock_size) + ',' + str(rocksdb_compression) + ',' +  str(snapshot_url) + str('\n')
            fhand = open(filename, 'a' )
            fhand.write(details_for_spreadsheet)
            fhand.close()   
     
  list_blobs(bucket,filename)

# importing libraries

import os
os.system('pip3 install pandas')

# importing pandas

import pandas as pd
  
# merging two csv files

df = pd.concat(
    map(pd.read_csv, ['detail_of_spreadsheet0.csv', 'detail_of_spreadsheet1.csv','detail_of_spreadsheet2.csv']), ignore_index=True)
print(df)

# df = pd.concat(
# map(pd.read_csv, ['detail_of_spreadsheet0.csv', 'detail_of_spreadsheet1.csv']), ignore_index=True)
# print(df)

# import glob
# import pandas as pd

# # merging the files

# joined_files = os.path.join("/home/joe_solana_com/test/ledger-spreadsheet/ledger-spreadsheet", "detail_of_spreadsheet*.csv")
  
# # A list of all joined files is returned

# joined_list = glob.glob(joined_files)
  
# # Finally, the files are joined

# df = pd.concat(map(pd.read_csv, joined_list), ignore_index=True, axis=1)
# #print(df)

df.to_csv('detail_of_spreadsheet.csv')

os.system('pip3 install gspread')
os.system('pip3 install oauth2client')
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# define the scope

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
# add credentials to the account

creds = ServiceAccountCredentials.from_json_keyfile_name('solana-devnet-507716491791.json', scope)
# authorize the clientsheet

client = gspread.authorize(creds)
# get the instance of the Spreadsheet

sheet = client.open('Untitled spreadsheet')
print('sheet id is : ',sheet.id)

with open('detail_of_spreadsheet.csv', 'r') as file_obj:
    content = file_obj.read()
    client.import_csv(sheet.id, data=content)

sheet_instance = sheet.get_worksheet(0)
sheet_instance.sort((6, 'asc'))
rows_count = sheet_instance.row_count
print('rows_count is :',rows_count)

rows_count_for_func = 'F2:F'+ str(rows_count)
val_F = sheet_instance.get_values(rows_count_for_func)
# print('val_F is :',val_F)
print('numbered rows count is: ',len(val_F))
rows_count_for_func = 'G1:G'+ str(int(rows_count)-1)
val_G = sheet_instance.get_values(rows_count_for_func)
# print('val_G is :',val_G)


# for y in range(len(bucket_name)):
#   worksheet_id = ['409361604','928311710']
#   csv_name = 'detail_of_spreadsheet' + str(y)+ '.csv'
#   print('csv_name is : ',csv_name)
#   with open(csv_name, 'r') as file_obj:
#     content = file_obj.read()
#     client.import_csv(sheet.id, data=content)
#     print('worksheet_id {} is used '.format(worksheet_id[y]))
# # get the first sheet of the Spreadsheet
#   sheet_instance = sheet.get_worksheet(y)
#   sheet_instance.sort((1, 'asc'))
#   rows_count = sheet_instance.row_count
#   print('rows_count is :',rows_count)
#   rows_count_for_func = 'E2:E'+ str(rows_count)
#   val_E = sheet_instance.get_values(rows_count_for_func)
#   print('val_E is :',val_E)
#   rows_count_for_func = 'F1:F'+ str(int(rows_count)-1)
#   val_F = sheet_instance.get_values(rows_count_for_func)
#   print('val_F is :',val_F)

gaps = []
# rows_count = sheet_instance.row_count
# print(rows_count)

rows_count = len(val_F)
print('rows_count is : ',rows_count)

for x in range(int(rows_count)):
    if not val_F[x][0].isnumeric() or not val_G[x][0].isnumeric():
        gaps.append([" "])
        continue
    else:
        gap_content = int(val_G[x][0]) - int(val_F[x][0])
        if int(gap_content) > 0:
            gaps.append([" "])
        else:
            gaps.append([str(gap_content)])

final_row_count = len(gaps)
print('final_row_count is : ', final_row_count)

final_rows_count_for_func = 'H1:H'+ str(int(final_row_count))
sheet_instance.update(final_rows_count_for_func,gaps)

        # index_start = 'A'+str(x+1)
        # index_end = 'A'+str(x+2)
        # column_count =  str(index_start) + ':' + str(index_end) 
        # slot_ = sheet_instance.get_values(column_count)
        # print(slot_)
        # start_search_slot = slot_[0][0]
        # end_search_slot = slot_[1][0]
        # new_new2spreadsheet_script.list_blobs(start_search_slot,end_search_slot,bucket) 
