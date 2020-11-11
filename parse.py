import json

def cleanStr4SQL(s):
    return s.replace("'","`").replace("\n"," ")

def esc(s):
    return s.replace("'","''").replace('\"',"''")

def int2BoolStr (value):
    if value == 0:
        return 'False'
    else:
        return 'True'

def parseBusinessData():
    with open('yelp_business.JSON','r') as f: 
        outfile =  open('./yelp_business.SQL', 'w') 
        line = f.readline()
        count_line = 0
        
        while line:
        #sql list
            data = json.loads(line)
            categoriesvalues = []
            attributekeys = []
            attributevalues = []
            hourskeys = []
            hoursvalues = []
            businesskeys = []
            businessvalues = []
            for i in data.keys():
                if i == "categories":
                    categoriesvalues = data[i].split(",")
                elif i == "attributes":
                    for j in data[i].keys():
                        if type(data[i][j]) == str or type(data[i][j]) == float or type(data[i][j]) == int:
                            attributekeys.append("'"+j.strip()+"'")
                            attributevalues.append("'"+str(data[i][j]).strip()+"'")
                        else:
                            for k in data[i][j].keys():
                                attributekeys.append("'"+k.strip()+"'")
                                attributevalues.append("'"+str(data[i][j][k]).strip()+"'")                                       
                elif i == "hours":
                    for j in data[i].keys():
                        hourskeys.append(j.strip())
                        hoursvalues.append(data[i][j].split("-"))
                else:
                    businesskeys.append(i)
                    if type(data[i]) == int:
                        if i == "is_open":
                            businessvalues.append("'"+int2BoolStr(data[i]).strip()+"'")
                        else:
                            businessvalues.append("'"+str(data[i]).strip()+"'")
                    elif type(data[i]) == float:
                        businessvalues.append("'"+str(data[i]).strip()+"'")
                    elif i == "":
                        businessvalues.append("'NULL'")
                    elif i == "name":
                        businessvalues.append("'"+esc(cleanStr4SQL(str(data[i]))).strip()+"'")
                    else:                        
                        businessvalues.append("'"+esc(str(data[i]).strip())+"'")
                        
            businesskeys = ','.join(map(str, businesskeys)) 
            businessvalues = ','.join(map(str, businessvalues)) 
            sql_str = "INSERT INTO businesstable ("+ businesskeys+",numCheckins,numTips) VALUES (" +businessvalues+ ","+ "'0'"+","+"'0'"+ ");"
            
            outfile.write(sql_str+'\n')
            
            for x in categoriesvalues:
                sql_str = "INSERT INTO categoriestable (business_id,category_name) VALUES ("+("'"+cleanStr4SQL(str(data["business_id"]))+"'") + ",'" + cleanStr4SQL(x).strip() + "');"
                outfile.write(sql_str+'\n')
            
            attributes = dict(zip(attributekeys,attributevalues))
            for x in attributes.keys():
                sql_str = "INSERT INTO attributestable (business_id,attribute,value) VALUES ("+("'"+cleanStr4SQL(str(data["business_id"]))+"'") + "," + x + "," +attributes[x] + ");"
                outfile.write(sql_str+'\n')
            hours = dict(zip(hourskeys,hoursvalues)) 
            for x in hours:
                sql_str = "INSERT INTO hourstable (business_id,dayofweek,open,close) VALUES ("+("'"+cleanStr4SQL(str(data["business_id"]))+"'") + "," +("'"+cleanStr4SQL(str(x))+"'") + ","+("'"+hours[x][0]+"'") + ","+("'"+hours[x][1]+"'") +  ");"
                outfile.write(sql_str+'\n')
            line = f.readline()
            count_line +=1  
                
    print(count_line)
    outfile.close()  
    f.close()
    
def parseUserData():
    #read the JSON file
    with open('yelp_user.JSON','r') as f:  #Assumes that the data files are available in the current directory. If not, you should set the path for the yelp data files.
        outfile =  open('./yelp_user.SQL', 'w')  #uncomment this line if you are writing the INSERT statements to an output file.
        line = f.readline()
        count_line = 0

        #connect to yelpdb database on postgres server using psycopg2
        #TODO: update the database name, username, and password
#        try:
#            conn = psycopg2.connect("dbname='yelpdb' user='postgres' host='localhost' password='admin'")
#        except:
#            print('Unable to connect to the database!')
#        cur = conn.cursor()
        while line:
            userkeys = []
            uservalues = []
            friendlist = []
            data = json.loads(line)
            for i in data.keys():
                if type(data[i]) == str or type(data[i]) == float or type(data[i]) == int: #json
                    userkeys.append(i)
                    uservalues.append(("'"+cleanStr4SQL(str(data[i]))+"'"))
                else:
                    friendlist = data[i] #list of friends 
                        
            attributes = ','.join(map(str, userkeys)) 
            values = ','.join(map(str, uservalues)) 
            sql_str = "INSERT INTO usertable ("+ attributes+",totalLikes) VALUES (" + values + ","+("'0'")+");"
#            try:
#                cur.execute(sql_str)
#            except:
#                print("Insert to userTABLE failed!")
#            conn.commit()
            # optionally you might write the INSERT statement to a file.
            outfile.write(sql_str+'\n')
            for x in friendlist:
                sql_str = "INSERT INTO friendtable (user_id,friend_id) VALUES ("+("'"+str(data["user_id"])+"'")+","+("'"+x+"'")+");"
                outfile.write(sql_str+'\n')
            line = f.readline()
            count_line +=1   
            
#    cur.close()
#    conn.close()
    print(count_line)
    outfile.close()  #uncomment this line if you are writing the INSERT statements to an output file.
    f.close()
    
def parseCheckinData():
    #read the JSON file
    with open('yelp_checkin.JSON','r') as f:  #Assumes that the data files are available in the current directory. If not, you should set the path for the yelp data files.
        outfile =  open('./yelp_checkin.SQL', 'w')  #uncomment this line if you are writing the INSERT statements to an output file.
        line = f.readline()
        count_line = 0

        #connect to yelpdb database on postgres server using psycopg2
        #TODO: update the database name, username, and password
#        try:
#            conn = psycopg2.connect("dbname='yelpdb' user='postgres' host='localhost' password='admin'")
#        except:
#            print('Unable to connect to the database!')
#        cur = conn.cursor()
        while line:
            busid = ""
            date = []
            data = json.loads(line)
            for i in data.keys():
                if i == "date":
                    date = data[i].split(",")
                else:
                    busid = data[i]
            for x in date:
                c = x.split()
                year = c[0][0:4]
                month = c[0][5:7]
                day = c[0][8:10]
                time = c[1]
                sql_str = "INSERT INTO checkintable (business_id,year,month,day,time) VALUES (" +("'"+ busid + "'") +","+("'"+  year+ "'") +","+ ("'"+month + "'")+ ","+ ("'"+day + "'")+","+ ("'"+time + "'")+");"
                outfile.write(sql_str+'\n')
#            try:
#                cur.execute(sql_str)
#            except:
#                print("Insert to checkinTABLE failed!")
#            conn.commit()
            # optionally you might write the INSERT statement to a file.
           

            line = f.readline()
            count_line +=1   
            
#    cur.close()
#    conn.close()
    print(count_line)
    outfile.close()  #uncomment this line if you are writing the INSERT statements to an output file.
    f.close()
    
def parseTipData():
    #read the JSON file
    with open('yelp_tip.JSON','r') as f:  #Assumes that the data files are available in the current directory. If not, you should set the path for the yelp data files.
        outfile =  open('./yelp_tip.SQL', 'w')  #uncomment this line if you are writing the INSERT statements to an output file.
        line = f.readline()
        count_line = 0

        #connect to yelpdb database on postgres server using psycopg2
        #TODO: update the database name, username, and password
#        try:
#            conn = psycopg2.connect("dbname='yelpdb' user='postgres' host='localhost' password='admin'")
#        except:
#            print('Unable to connect to the database!')
#        cur = conn.cursor()
        while line:
            Tipkeys = []
            Tipvalues = []
            data = json.loads(line)
            for i in data.keys():
                Tipkeys.append(i)
                if i == "text":
                    Tipvalues.append(("'"+esc(cleanStr4SQL(str(data[i])))+"'"))
                else:
                    Tipvalues.append(("'"+cleanStr4SQL(str(data[i]))+"'"))
            attributes = ','.join(map(str, Tipkeys)) 
            values = ','.join(map(str, Tipvalues)) 
            sql_str = "INSERT INTO tiptable ("+ attributes+") VALUES (" + values + ");"
#            try:
#                cur.execute(sql_str)
#            except:
#                print("Insert to tipTABLE failed!")
#            conn.commit()
            # optionally you might write the INSERT statement to a file.
            outfile.write(sql_str+'\n')

            line = f.readline()
            count_line +=1   
            
#    cur.close()
#    conn.close()
    print(count_line)
    outfile.close()  #uncomment this line if you are writing the INSERT statements to an output file.
    f.close()

parseBusinessData()
parseUserData()
parseCheckinData()
parseTipData()
