import pandas as pd
from sqlalchemy import create_engine, text, Column, Integer, String, Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

username = ' ' #assume proper username
password = ' ' #assume proper password
host = 'localhost'
port = '5432'
database = 'postgres'

engine = create_engine('postgresql+psycopg2://user:pass@127.0.0.1:5432/postgres')

# Try to connect and execute a simple query
try:
    # Connect to the database
    with engine.connect() as connection:
        # Execute a test query
        result = connection.execute(text("SELECT version();"))
        # Fetch the result
        version = result.fetchone()
        print(f"Database connection successful. PostgreSQL version: {version[0]}")
except Exception as e:
    print(f"An error occurred: {e}")

# Read Excel file into a DataFrame
file_path = "C:person.csv" #wherever this is for you
df_person = pd.read_csv(file_path)

# Define the age range mapping
age_range_mapping = {
    '12 or 13 years old': 1,
    '14 or 15 years old': 2,
    '16 or 17 years old': 3,
    'Between 18 and 20 years old': 4,
    'Between 21 and 23 years old': 5,
    '24 or 25 years old': 6,
    'Between 26 and 29 years old': 7,
    'Between 30 and 34 years old': 8,
    'Between 35 and 49 years old': 9,
    'Between 50 and 64 years old': 10,
    '65 years old or older': 11
}

df_person['age_range_id'] = df_person['Age'].map(age_range_mapping)

Base = declarative_base()

class Person(Base):
    __tablename__ = 'person'
    person_id = Column(Integer, primary_key=True)
    gender = Column(Integer)
    age_range_id = Column(Integer)
    rural_urban_id = Column(Integer)
    source_dataset = Column(Integer)
    year = Column(Integer)

# Gender mapping
gender_mapping = {
    'Male': 1,
    'Female': 2
}

# Rural/Urban mapping - Assuming you have these mappings defined similarly
rural_urban_mapping = {
    'Large Metro': 1,
    'Small Metro': 2,
    'Nonmetro': 3
}

# Bind the sessionmaker to the engine
Session = sessionmaker(bind=engine)

# Create a session instance
session = Session()

try:
    for index, row in df_person.iterrows():
        # Create an instance of Person for each row in the DataFrame
        entry = Person(
            person_id=row['PersonID'],
            gender=gender_mapping.get(row['Sex'], None),
            age_range_id=row['age_range_id'],
            #rural_urban_id=rural_urban_mapping.get(row['RuralStatus'], None) #doesn't work if i include this, some foreign key violation
            source_dataset=None,  # Assuming you need to fill this based on some logic or external mapping
            year=2022  # Assuming this is the year of the data
        )
        
        # Add each instance to the session
        session.add(entry)
    
    # Commit all changes
    session.commit()
    print("Data inserted successfully.")
except Exception as e:
    # Rollback the session in case of an error
    session.rollback()
    print(f"An error occurred during insertion: {e}")
finally:
    # Close the session
    session.close()