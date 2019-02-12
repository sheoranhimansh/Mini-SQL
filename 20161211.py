import os
import sys
import sqlparse
from statistics import mean
from collections import defaultdict
import operator
ops = {'>':operator.gt,'<':operator.lt,'>=':operator.ge,'<=':operator.le,'=':operator.eq}
arr = ['max','min','Sum','average']
cond = ['<','>','=','<=','>=']
def dbInfo(filename):
    metadata = defaultdict(list)
    try:
        with open(filename,'r') as file:
            content = file.readlines()
        content = [x.strip() for x in content]
        for i in range(len(content)):
            column = []
            if content[i] == '<begin_table>':
                i+=1
                tablename = content[i]
                i+=1
                while content[i] != '<end_table>':
                    column.append(content[i])
                    i+=1
                metadata[tablename]=column
    except:
        print("Some Error Occured. Please Try Again")
    return metadata

def quotes_removal(s):
    s = s.strip()
    while len(s) > 1 and (s[0]=='\"' or s[0]=='\'') and s[0]==s[-1]:
        s = s[1:-1]
    return s

def tableInfo(filename,metadata):
    tabledict = []
    try:
        with open('./files/'+filename+'.csv') as table:
            tdata = table.readlines()
        tdata = [x.strip() for x in tdata]
        if len(metadata) == len(tdata[0].split(',')):
            for i in range(len(tdata)):
                new_data = []
                data = tdata[i].split(',')
                for d in data:
                    d = quotes_removal(d)
                    new_data.append(d)
                tabledict.append(new_data)
                temp = tabledict[i][-1]
                tabledict[i][-1] = temp[:len(temp)]
        else:            print('Please update METADATA Dictionary')
    except:        print('Some Error Occured.Please Try Again')
    return tabledict

def attributeInfo(filename):
    attributedata = defaultdict()
    try:
        with open(filename,'r') as file:            content = file.readlines()
        content = [x.strip() for x in content]
        for i in range(len(content)):
            if content[i] == '<begin_table>':
                i+=1
                tablename = content[i]
                i+=1
                while content[i] != '<end_table>':
                    attributedata[content[i]] = tablename
                    i+=1
    except:
        print("Some Error Occured. Please Try Again")
    return attributedata

def validate(query):
    query = query.split()
    if query[1]!='distinct':
        if query[0] == 'select' and query[2] == 'from':            return True
        else:
            print('INVALID FORMAT. Correct Usage -> [(select,from,where) are in lowercase')
            sys.exit()
    else:
        if query[0] == 'select' and query[3] == 'from':            return True
        else:
            print('INVALID FORMAT. Correct Usage -> [(select,from,where) are in lowercase')
            sys.exit()

def joinTable(table1,table2):
    final = []
    if len(table1) == 0:
        return table1
    for i in table1:
        for j in table2:
            final.append(i+','+j)
    return final

def joinTable1(table1,table2):
    final = []
    if len(table1) == 0:
        return table2
    for i in table1:
        for j in table2:
            final.append(i+','+j)
    return final

def unique(list1):
    unique_list = []
    for x in list1:
        if x not in unique_list:
            unique_list.append(x)
    return unique_list

def processQuery(query):
    if validate(query):
        query = query.split()
        if query[1] != 'distinct':
            try:
                tablenames = query[3].split(',')
            except:
                print("Invalid Form Of Input,Check Query")
                sys.exit()
            for name in tablenames:
                if name not in tabledata:
                    print('The Following table '+name+' Does not Exist ')
                    sys.exit()
        elif query[1] == 'distinct':
            tablename = query[4]
            if tablename not in tabledata:
                print('The following table'+tablename+' Does not exist')
                sys.exit()

        if len(query) == 4:
            if (query[1] == '*'):
                tablename = query[3].split(',')
                ans = []
                a = []
                for table in tablename:
                    temp = []
                    for row in tabledata[table]:
                        str = ''
                        for r in row:
                            str += r+','
                        str = str[:-1]
                        temp.append(str)
                    ans.append(temp)
                if len(tablename) == 1:
                    str = ''
                    for i in metadata[tablename[0]]:
                        str+= tablename[0]+'.'+i+','
                    print(str[:-1])
                    for i in ans:
                        for j in i:
                            print(j)
                else:
                    i = 0
                    a = ans[0]
                    while(i<len(ans)-1):
                        a =joinTable(a,ans[i+1])
                        i+=1
                    str = ''
                    for table in tablename:
                        for i in metadata[table]:
                            str += table+'.'+i+','
                    print(str[:-1])
                    for i in a:
                        print(i)
                sys.exit()

            if query[1].split('(')[0] in arr:
                val = query[1].split('(')[1].split(')')[0]
                tablename = query[3]
                try:  index = metadata[tablename].index(val)
                except:
                    print("Given Column Does Not Exist In The Given Table")
                    sys.exit()
                col = []
                for data in tabledata[tablename]:
                    col.append(int(data[index]))
                print(col)
                if query[1].split('(')[0] == 'max':
                    ans = max(col)
                    print('max('+tablename+'.'+val+')')
                    print(ans)
                    sys.exit()
                if query[1].split('(')[0] == 'min':
                    ans = min(col)
                    print('min('+tablename+'.'+val+')')
                    print(ans)
                    sys.exit()
                if query[1].split('(')[0] == 'Sum':
                    ans = sum(col)
                    print('Sum('+tablename+'.'+val+')')
                    print(ans)
                    sys.exit()
                if query[1].split('(')[0] == 'average':
                    ans = mean(col)
                    print('average()'+tablename+'.'+val+')')
                    print(ans)
                    sys.exit()

            else:
                col_list = query[1].split(',')
                tablenames = query[3].split(',')
                tabledict = defaultdict()
                for col in col_list:
                    flag = 0
                    for table in tablenames:
                        if col in metadata[table]:
                            tabledict[col]=table
                            flag = 1
                    if flag == 0:
                        print('Given Columns not found in given table')
                        sys.exit()
                if len(tablenames)==1:
                    col_dict = defaultdict()
                    str = ''
                    for col in col_list:
                        str += tabledict[col]+'.'+col+','
                        temp = []
                        ind = metadata[tablenames[0]].index(col)
                        for data in tabledata[tablenames[0]]:
                            temp.append(data[ind])
                        col_dict[col]=temp
                    print(str[:-1])
                    for i in range(len(col_dict[col])):
                        str = ''
                        for col in col_list:
                            str += col_dict[col][i]+','
                        str = str[:-1]
                        print(str)
                else:
                    str = ''
                    for col in col_list:
                        str += tabledict[col]+'.'+col+','
                    print(str[:-1])
                    final_join = []
                    for table in tablenames:
                        temp = []
                        for i in tabledata[table]:
                            i = ','.join(i)
                            temp.append(i)
                        final_join = joinTable1(final_join,temp)
                    count = 0
                    ind_dict = defaultdict()
                    for table in tablenames:
                        for i in metadata[table]:
                            ind_dict[i]=count
                            count +=1
                    for line in final_join:
                        line = line.split(',')
                        str = ''
                        for col in col_list:
                            str+= line[ind_dict[col]]+','
                        print(str[:-1])
        elif len(query) == 5:
            col_dict = defaultdict()
            tablename = query[4]
            col_list = query[2].split( ',')
            for col in col_list:
                try:
                    index = metadata[tablename].index(col)
                except:
                    print(col+"<- this col does not belong in the given table")
                    sys.exit()
                temp = []
                for data in tabledata[tablename]:
                    temp.append(data[index])
                col_dict[col] = temp
            str = ''
            for i in col_list:       str += tablename+'.'+i+','
            print(str[:-1])
            temp = []
            for i in range(len(col_dict[col])):
                str = ''
                for col in col_list:
                    str += col_dict[col][i]+','
                str = str[:-1]
                temp.append(str)
            temp = unique(temp)
            for i in temp: print(i)
        elif len(query)>6:
            col_list = query[1].split(',')
            tablenames = query[3].split(',')
            tabledict = defaultdict()
            flag1=0
            flag2= 0
            for c in list(attribute.keys()):
                if query[5].find(c) != -1:
                    flag1 = 1
                    condition1 = query[5].split(c)[1]
                if query[7].find(c) != -1:
                    flag2 = 1
                    condition2 = query[7].split(c)[1]
            if flag1 == 0 or flag2 == 0:
                print('given column does not exist in the tables mentioned')
                sys.exit()
            if ord(condition1[1]) == 61:
                condition1 = condition1[0] + condition1[1]
            else:
                condition1 = condition1[0]
            col1 = query[5].split(condition1)[0]
            value1 = query[5].split(condition1)[1]
            if ord(condition2[1]) == 61:
                condition2 = condition2[0] + condition2[1]
            else:
                condition2 = condition2[0]
            value2 = query[7].split(condition2)[1]
            col2 = query[7].split(condition2)[0]
            clist = [col1,col2]
            if query[1] != '*':
                for col in col_list:
                    flag = 0
                    for table in tablenames:
                        if col in metadata[table]:
                            tabledict[col]=table
                            flag = 1
                    if flag == 0:
                        print('Given Columns not found in given table')
                        sys.exit()
                str = ''
                for col in col_list:
                    str += tabledict[col]+'.'+col+','
                print(str[:-1])
            else:
                for c in clist:
                    flag = 0
                    for table in tablenames:
                        if c in metadata[table]:
                            flag = 1
                    if flag == 0:
                        print('Given Column not found in the given table')
                        sys.exit()
                str = ''
                for table in tablenames:
                    for col in metadata[table]:
                        str += table+'.'+col+','
                print(str[:-1])
            final_join = []
            for table in tablenames:
                temp = []
                for i in tabledata[table]:
                    i = ','.join(i)
                    temp.append(i)
                final_join = joinTable1(final_join,temp)
            count = 0
            ind_dict = defaultdict()
            for table in tablenames:
                for i in metadata[table]:
                    ind_dict[i]=count
                    count +=1
            if query[6] == 'AND':
                if query[1] == '*':
                    try:
                        for line in final_join:
                            line = line.split(',')
                        ops[condition1](line[ind_dict[col1]],value1) and ops[condition2](line[ind_dict[col2]],value2)
                    except:
                        print('given column does not exist in the given tables')
                        sys.exit()
                    for line in final_join:
                        line = line.split(',')
                        if ops[condition1](int(line[ind_dict[col1]]),int(value1)) and ops[condition2](int(line[ind_dict[col2]]),int(value2)):
                            print((',').join(line))

                else:
                    for line in final_join:
                        line = line.split(',')
                        str = ''
                        try:
                            if ops[condition1](int(line[ind_dict[col1]]),int(value1)) and ops[condition2](int(line[ind_dict[col2]]),int(value2)):
                                for col in col_list:
                                    str+= line[ind_dict[col]]+','
                                print(str[:-1])
                        except:
                            print('given column does not exist in the given tables')
                            sys.exit()
            if query[6] == 'OR':
                if query[1] == '*':
                    try:
                        for line in final_join:
                            line = line.split(',')
                        ops[condition1](line[ind_dict[col1]],value1) or ops[condition2](line[ind_dict[col2]],value2)
                    except:
                        print('given column does not exist in the given tables')
                        sys.exit()
                    for line in final_join:
                        line = line.split(',')
                        if ops[condition1](int(line[ind_dict[col1]]),int(value1)) or ops[condition2](int(line[ind_dict[col2]]),int(value2)):
                            print((',').join(line))
                else:
                    for line in final_join:
                        line = line.split(',')
                        str = ''
                        try:
                            if ops[condition1](int(line[ind_dict[col1]]),int(value1)) or ops[condition2](int(line[ind_dict[col2]]),int(value2)):
                                for col in col_list:
                                    str+= line[ind_dict[col]]+','
                                print(str[:-1])
                        except:
                            print('given column Does not exist in the given tables')
                            sys.exit()
            elif query[6] != 'AND' and query[6] != 'OR':
                print('Only AND/OR operator allowed')
        elif len(query) == 6:
            tablenames[0] = query[5].split('=')[0].split('.')[0]
            tablenames[1] = query[5].split('=')[1].split('.')[0]
            col1 = query[5].split('=')[0].split('.')[1]
            col2 = query[5].split('=')[1].split('.')[1]
            index1 = metadata[tablenames[0]].index(col1)
            index2 = metadata[tablenames[1]].index(col2)
            table1,table2 = [],[]
            for i in range(len(tabledata[tablenames[0]])):
                temp = tabledata[tablenames[0]][i]
                temp = ','.join(temp)
                table1.append(temp)
            for i in range(len(tabledata[tablenames[1]])):
                temp = tabledata[tablenames[1]][i]
                temp = ','.join(temp)
                table2.append(temp)
            arr1,arr2 = [],[]
            for data in tabledata[tablenames[0]]:
                arr1.append(data[index1])
            for data in tabledata[tablenames[1]]:
                arr2.append(data[index2])
            finalans = []
            if (query[1] == '*'):
                for i in range(len(arr1)):
                    for j in range(len(arr2)):
                        if arr1[i]==arr2[j]:
                            finalans.append(table1[i]+','+table2[j])
                finalans = unique(finalans)
                str = ''
                for t in tablenames:
                    for key in metadata[t]:
                        str += t+'.'+key+','
                print(str[:-1])
                for i in finalans:
                    print(i)
                sys.exit()
            else:
                temp_dict = defaultdict()
                str = ''
                col_list = query[1].split(',')
                tablenames = query[3].split(',')
                for i in range(len(col_list)):
                    for j in range(len(tablenames)):
                        if col_list[i] in metadata[tablenames[j]]:
                            str += tablenames[j]+'.'+col_list[i]+','
                            temp_dict[col_list[i]] = tablenames[j]
                            break
                print(str[:-1])
                for i in range(len(arr1)):
                    for j in range(len(arr2)):
                        if arr1[i] == arr2[j]:
                            str = ''
                            for col in col_list:
                                index = metadata[temp_dict[col]].index(col)
                                if temp_dict[col] == tablenames[0]:
                                    str+= tabledata[temp_dict[col]][i][index]+','
                                else:
                                    str+= tabledata[temp_dict[col]][j][index]+','
                            print(str[:-1])

if __name__ == '__main__':
    metadata = dbInfo('files/metadata.txt')
    attribute = attributeInfo('files/metadata.txt')
    tabledata = defaultdict()
    for name in metadata:
        tabledata[name] = tableInfo(name,metadata[name])
    command = sys.argv[1]
    command = quotes_removal(command)
    if command[-1] != ';':
        print("please put ; at the end of the query")
        sys.exit()
    query = command[:-1].strip()
    query1 =''
    for i in range(len(query)):
        if query[i]== ' ' and query[i+1]== ',':
            continue
        if query[i]== ' ' and query[i-1]== ',':
            continue
        if query[i]== ' 'and query[i+1] in cond:
            continue
        if query[i]== ' 'and query[i-1] in cond:
            continue
        else:
            query1+= query[i]
    processQuery(query1)
