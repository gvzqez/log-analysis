#!/usr/bin/env python

import psycopg2

DBNAME = "news"


with open("output.txt", "w") as file:
    file.write(" ----- Log Analysis ----- " + '\n\n')


queries = []

queries.append('''
                SELECT CONCAT('"', title, '", By ', author, ' - ', views, ' views') as top_articles
                FROM (
                     select art.title, a.name as author, count(*) as views
                     from articles art
                     join authors a on art.author = a.id
                     join log on substring(log.path from 10) = art.slug
                     group by art.title, a.name
                     order by views desc
                     limit 3
                     ) t
                    ;
               '''
               )

queries.append('''
                SELECT CONCAT(author, ' - ', views, ' views') as top_authors
                FROM (
                     select a.name as author, count(*) as views
                     from authors a
                     join articles art on a.id = art.author
                     join log on substring(log.path from 10) = art.slug
                     group by a.name
                     order by views desc
                     ) t
                ;
               '''
               )

queries.append('''
                SELECT CONCAT(day, ' - ', round(cast(errorsPercent as numeric), 2), '% errors')
                FROM (
                     select a.day, (cast(b.totalerrors as float) * 100) / (a.totalok + b.totalerrors) as errorsPercent
                     from (
                           select date(time) as day, count(*) as totalok
                           from log where status = '200 OK'
                           group by day
                           ) a
                     join (
                           select date(time) as day, count(*) as totalerrors
                           from log where status = '404 NOT FOUND'
                           group by day
                           ) b on a.day = b.day
                     where (cast(b.totalerrors as float) * 100) / (a.totalok + b.totalerrors) > 1
                     order by day desc
                     ) t
                ;
               '''
               )


def get_analysis(query):
    # Execute queries and write output
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(query)
    data = c.fetchall()
    db.close()

    with open("output.txt", "a") as file:
        for line in data:
            file.write(line[0] + '\n')

        file.write('\n')


for query in queries:
    get_analysis(query)


with open("output.txt") as file:
    print file.read()
