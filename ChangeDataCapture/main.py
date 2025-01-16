import faker
import psycopg2
from datetime import datetime
#fake.date_time_this_year()

#fake = faker.Fake()
fake = faker.Faker()

def generate_customer():
    user = fake.simple_profile()

    return {
        "id": fake.uuid4(),
        "name": fake.name(),
        'address': fake.address(),
        'email': fake.email(),
        'phone_number': fake.phone_number(),
        'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=99),
        "timestamp": datetime.utcnow().timestamp(),
    }

def create_table(conn):
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS customer_profile (
        customer_id varchar(255),
        customer_name varchar(255),
        customer_address varchar(255),
        customer_email varchar(255),
        customer_phone_number varchar(25),
        customer_date_of_birth varchar(255),
        origination_DT TIMESTAMP
        )
        """
    )

    cursor.close()
    conn.commit()

if __name__ == "__main__":
    conn = psycopg2.connect(
        host='localhost',
        database='customers',
        user='postgres',
        password='postgres',
        port=5432
    )

    create_table(conn)
    for i in range(10):
        customer = generate_customer()

        cursor = conn.cursor()
        cursor.execute(
            """
            insert into customer_profile (customer_id, customer_name, customer_address,customer_email, customer_phone_number 
            ,customer_date_of_birth, origination_DT) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (customer["id"], customer["name"], customer["address"], customer["email"], customer["phone_number"]
                     , customer["date_of_birth"], datetime.fromtimestamp(customer["timestamp"]).strftime('%Y-%m-%d %H:%M:%S'))
        )
        cursor.close()
        print(i)
    conn.commit()


import faker
import psycopg2
from datetime import datetime
#fake.date_time_this_year()

fake = faker.Faker()

def generate_customer():
    user = fake.simple_profile()

    return {
        "id": fake.uuid4(),
        "name": fake.name(),
        'address': fake.address(),
        'email': fake.email(),
        'phone_number': fake.phone_number(),
        'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=99),
        "timestamp": datetime.utcnow().timestamp(),
    }

def create_table(conn):
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS customer_profile (
        customer_id varchar(255),
        customer_name varchar(255),
        customer_address varchar(255),
        customer_email varchar(255),
        customer_phone_number varchar(25),
        customer_date_of_birth varchar(255),
        origination_DT TIMESTAMP
        )
        """
    )

    cursor.close()
    conn.commit()

if __name__ == "__main__":
    conn = psycopg2.connect(
        host='localhost',
        database='customers',
        user='postgres',
        password='postgres',
        port=5432
    )

    create_table(conn)
    for i in range(10):
        customer = generate_customer()

        cursor = conn.cursor()
        cursor.execute(
            """
            insert into customer_profile (customer_id, customer_name, customer_address,customer_email, customer_phone_number 
            ,customer_date_of_birth, origination_DT) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (customer["id"], customer["name"], customer["address"], customer["email"], customer["phone_number"]
                     , customer["date_of_birth"], datetime.fromtimestamp(customer["timestamp"]).strftime('%Y-%m-%d %H:%M:%S'))
        )
        cursor.close()
        print(i)
    conn.commit()


