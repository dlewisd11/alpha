import os
import logsetup as ls
import mysql.connector


dbTableName = str(os.getenv('DB_TABLE_NAME'))


try:
    db = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PW'),
        database=os.getenv('DB_NAME')
    )
except:
    ls.log.error("Error connecting to database, quitting program.")
    ls.log.exception("database")
    quit()


def runQuery(query, values):
    try:
        dbCursor = db.cursor()
        dbCursor.execute(query, values)
        db.commit()
    except:
        ls.log.exception("database.runQuery")


def runQueryAndReturnResults(query, values):
    try:
        dbCursor = db.cursor()
        dbCursor.execute(query, values)
        return dbCursor.fetchall()
    except:
        ls.log.exception("database.runQueryAndReturnResults")
        