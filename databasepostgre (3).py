import psycopg2
from tabulate import tabulate

# Connect to PostgreSQL
con = psycopg2.connect(
    host="localhost",
    database="sarahcruz",
    user="sarahcruz",
    password="PASSWORD"
)

# Isolation + atomicity
con.set_isolation_level(3)   # SERIALIZABLE
con.autocommit = False

try:
    cur = con.cursor()

    # TRANSACTION 2

    # Delete depot d1 from Depot and Stock

    cur.execute("DELETE FROM stock WHERE depid = %s", ('d1',))
    cur.execute("DELETE FROM depot WHERE depid = %s", ('d1',))

    print("Transaction 2 completed successfully")

    # TRANSACTION 5

    # Add product (p100, cd, 5) in Product
    # Add product (p100, d2, 50) in Stock

    cur.execute(
        "INSERT INTO product (prodid, pname, price) VALUES (%s, %s, %s)",
        ('p100', 'cd', 5))
    cur.execute(
        "INSERT INTO stock (prodid, depid, quantity) VALUES (%s, %s, %s)",
        ('p100', 'd2', 50))
    print("Transaction 5 completed successfully")

    # TRANSACTION 6

    # Add depot (d100, Chicago, 100) in Depot
    # Add depot (p1, d100, 100) in Stock 

    cur.execute(
        "INSERT INTO depot (depid, addr, volume) VALUES (%s, %s, %s)",
        ('d100', 'Chicago', 100))

    cur.execute(
        "INSERT INTO stock (prodid, depid, quantity) VALUES (%s, %s, %s)",
        ('p2', 'd100', 100))

    print("Transaction 6 completed successfully")

    con.commit()

except (Exception, psycopg2.DatabaseError) as err:
    print(err)
    print("Transactions could not be completed so database will be rolled back")
    con.rollback()

finally:
    if con:
        cur.close()
        con.close()
        print("PostgreSQL connection is now closed")

