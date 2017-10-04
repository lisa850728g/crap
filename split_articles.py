import MySQLdb
import json
import jieba
import jieba.posseg as pseg

file = open('results.log', mode='w', encoding='utf-8')
db = MySQLdb.connect("139.162.23.125", "root", "DifficulT7130", "cybersite", use_unicode=True, charset="utf8")
cursor = db.cursor()

sql = "SELECT HEADLINES,CONTENT FROM fetch_data"
cursor.execute(sql)
split = ""
keywords = []
n = 1

results = cursor.fetchall()

for record in results:
    file.write("Success: %s\n" % record[0])
    print (record[0])
    words = pseg.cut(record[1])
    for word, flag in words:
        if "n" in flag:
            if word not in keywords:
                keywords.append(word)
                count = record[1].count(word)
                split += '"' + word + '":' + str(count) + ','
    whole = "{" + split[:-1] + "}"
    file.write("%s\n" % whole)
    cursor.execute("UPDATE fetch_data SET keywords = %s where id = %s",(whole,n))
    n += 1
    split = ""
    db.commit()
db.close()
file.close()
