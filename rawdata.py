# Stores every tagging instance.
import mysql.connector
from mysql.connector import errorcode
import numpy as np

DB_NAME = 'rawdata'

TABLES = {}
TABLES['itemtagcount'] = (
  "CREATE TABLE movie_tags( "
  "item int(10) NOT NULL, "
  "tag varchar(100) NOT NULL, "
  "count int(10) NOT NULL ); ")
  
TABLES['itemcount'] = (
  "CREATE TABLE movie_tags( "
  "item int(10) NOT NULL, "
  "count int(10) NOT NULL ); ")
  
TABLES['tagcount'] = (
  "CREATE TABLE movie_tags( "
  "tag varchar(100) NOT NULL, "
  "count int(10) NOT NULL ); ")

cnx = mysql.connector.connect(user='root', password='Reverie42')
cursor = cnx.cursor()

def create_database(cursor):
  try:
    cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
  except mysql.connector.Error as err:
     print "Failed creating database: {}".format(err)
     exit(1)

try:
    cnx.database = DB_NAME  
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        cnx.database = DB_NAME
    else:
        print err
        exit(1)

for name, ddl in TABLES.iteritems():
    try:
        print "Creating table {}: ".format(name)
        cursor.execute(ddl)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print "already exists."
        else:
            print err.msg
    else:
        print "OK"

cnx.commit()
cursor.close()
cnx.close()

def tag_item(item, tag):
  if len(tag) > 100:
    return

  cnx = mysql.connector.connect(user='root', password='Reverie42', buffered=True)
  cursor = cnx.cursor()
  cnx.database = DB_NAME
  
  update_itemtagcount = ("INSERT INTO itemtagcount "
             "(item, tag, count) "
             "VALUES (%s, %s, 1) "
             "ON DUPLICATE KEY UPDATE "
             "count = count + 1; ")
  
  update_itemcount = ("INSERT INTO itemcount "
             "(item, count) "
             "VALUES (%s, 1) "
             "ON DUPLICATE KEY UPDATE "
             "count = count + 1; ")
             
  update_tagcount = ("INSERT INTO tagcount "
             "(tag, count) "
             "VALUES (%s, 1) "
             "ON DUPLICATE KEY UPDATE "
             "count = count + 1; ")
             
  update_totalcount = "IF (SELECT @totalcount IS NULL) "
                      "THEN SET @totalcount = 0; "
                      "ELSE SET @totalcount = @totalcount + 1; "
                      "END IF; "
                      
  
  cursor.execute(update_itemtagcount, (item, tag))
  cursor.execute(update_itemcount, (item))
  cursor.execute(update_tagcount, (tag))
  cursor.execute(update_totalcount)
  
  cnx.commit()
  cursor.close()
  cnx.close()

def find_PPMI(item, tag):
  cnx = mysql.connector.connect(user='root', password='Reverie42', buffered=True)
  cursor = cnx.cursor()
  cnx.database = DB_NAME
  
  find_itemtagcount = ("SELECT count FROM itemtagcount "
                       "WHERE item = %s AND tag = %s; ")
  
  find_itemcount = ("SELECT count FROM itemcount "
                    "WHERE item = %s; ")
  
  find_tagcount = ("SELECT count FROM tagcount "
                   "WHERE tag = %s; ")
  
  find_totalcount = ("SELECT @totalcount; ")
  
  cursor.execute(find_totalcount)
  for (count) in cursor:
    total = count
  
  cursor.execute(find_itemtagcount, (item, tag))
  for (count) in cursor:
    pr_itemtag = float(count) / total
    
  cursor.execute(find_itemcount, (item))
  for (count) in cursor:
    pr_item = float(count) / total
    
  cursor.execute(find_tagcount, (tag))
  for (count) in cursor:
    pr_tag = float(count) / total
    
  pmi = np.log(pr_itemtag / (pr_item * pr_tag))
  ppmi = 2 ** (pmi + np.log(pr_itemtag))

  cnx.commit()
  cursor.close()
  cnx.close()
  
  return ppmi
