import psycopg2

def test_connectToPostgres(db, user, pw):
    c = False
    try:
        connection = psycopg2.connect(user=user, password=pw , database=db)
        cursor = connection.cursor()
        print("connected to postgres DB ")
        c = True
        assert c == True

    except(Exception, psycopg2.Error):
        print("postgres error ", psycopg2.Error)
        c = False
        assert c == True