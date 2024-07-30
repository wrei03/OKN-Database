import psycopg2
from sqlalchemy import create_engine, text, Column, Integer, String, Sequence
from sqlalchemy.orm import sessionmaker, declarative_base
#from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Database connection parameters
username = {username}
password = {password}
host = 'localhost'
port = '5432'
database = {database}

engine = create_engine('postgresql+psycopg2://{username}:{password}!@127.0.0.1:5432/{database}')

# Step 1: Define a declarative base and map the table
Base = declarative_base()

class Substance(Base):
    __tablename__ = 'substance'
    __table_args__ = {'schema': 'public'}  # Specify the schema if not the default
    substance_id = Column(Integer, primary_key=True, nullable=False)
    substance_code = Column(String(40))
    substance_name = Column(String(80))
    substance_schedule = Column(Integer, default=0)
    other_names = Column(String(80))
    source_dataset = Column(Integer)
    year = Column(Integer)
    parent_category = Column(Integer)

# Step 2: Create a session
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()
try:
    # Test the connection by executing a simple query
    with engine.connect() as connection:
        result = connection.execute(text("SELECT version();"))
        version = result.fetchone()
        print(f"Database connection successful. PostgreSQL version: {version[0]}")

    #delete if something is already there?
    session.execute(text("TRUNCATE TABLE public.substance RESTART IDENTITY CASCADE;"))
    session.commit()  # Commit the deletion operation

    print("Existing data cleared successfully.")
        
    # Step 3: Create the table if it doesn't exist (if you want to)
    #Base = declarative_base()

    # Step 4: Provide the data to insert as a list of Substance objects
    print ("Inserting")
    substances_to_insert = [
        Substance(substance_id=1, substance_code='N/A', substance_name='Nicotine', substance_schedule=-1, other_names='Not Available' , source_dataset=1, year=2022, parent_category=-1),
        #print ("Nicotine inserted"),
        Substance(substance_id=2, substance_code= 'N/A' , substance_name= 'Alcohol' , substance_schedule=-1, other_names= 'Not Available' , source_dataset=1, year=2022, parent_category=-1),
        #print ("Alcohol inserted"),
        Substance(substance_id=3, substance_code= '7360' , substance_name= 'Marijuana' , substance_schedule=1, other_names= 'Aunt Mary, Blunts, Mary Jane, Joint, Pot' , source_dataset=1, year=2022, parent_category=-1),
        #print ("Marijuana inserted"),
        Substance(substance_id=4, substance_code= 'N/A' , substance_name= 'Blunts' , substance_schedule=1, other_names= 'See Marijuana' , source_dataset=1, year=2022, parent_category=3),
        #print ("Blunts inserted"),
        Substance(substance_id=5, substance_code= '9041' , substance_name= 'Cocaine' , substance_schedule=2, other_names= 'Coke, Crack, Snow' , source_dataset=1, year=2022, parent_category=-1),
        #print ("Cocaine inserted"),
        Substance(substance_id=6, substance_code= 'N/A' , substance_name= 'Crack' , substance_schedule=2, other_names= 'See Cocaine' , source_dataset=1, year=2022, parent_category=5),
        #print ("Crack inserted"),
        Substance(substance_id=7, substance_code= '9200' , substance_name= 'Heroin' , substance_schedule=1, other_names= 'Big H, Hell Dust, Horse, Smack' , source_dataset=1, year=2022, parent_category=-1),
        #print ("Heroin inserted"),
        Substance(substance_id=8, substance_code= 'N/A' , substance_name= 'Hallucinogens' , substance_schedule=1, other_names= 'Acid, Fry, Mushrooms, X' , source_dataset=1, year=2022, parent_category=-1),
        #print ("Hallucinogens inserted"),
        Substance(substance_id=9, substance_code= 'N/A' , substance_name= 'Inhalants' , substance_schedule=-1, other_names= 'Huff, Rush, Whippets' , source_dataset=1, year=2022, parent_category=-1),
        #print ("Inhalants inserted"),
        Substance(substance_id=10, substance_code= '1105' , substance_name= 'Methamphetamine' , substance_schedule=2, other_names= 'Batu, Chalk, Meth, Speed' , source_dataset=1, year=2022, parent_category=-1),
        #print ("Methamphetamine inserted"),
        Substance(substance_id=11, substance_code= 'N/A' , substance_name= 'Pain Relievers' , substance_schedule=-1, other_names= 'Not Sourced' , source_dataset=1, year=2022, parent_category=-1),
        #print ("Pain Relievers inserted"),
        Substance(substance_id=12, substance_code= 'N/A' , substance_name= 'Tranquilizers' , substance_schedule=-1, other_names= 'Not Sourced' , source_dataset=1, year=2022, parent_category=-1),
        #print ("Tranquilizers inserted"),
        Substance(substance_id=13, substance_code= 'N/A' , substance_name= 'Stimulants' , substance_schedule=-1, other_names= 'Not Sourced' , source_dataset=1, year=2022, parent_category=-1),
        #print ("Stimulants inserted"),
        Substance(substance_id=14, substance_code= 'N/A' , substance_name= 'Sedatives' , substance_schedule=-1, other_names= 'Not Sourced' , source_dataset=1, year=2022, parent_category=-1),
        #print ("Sedatives inserted"),
        Substance(substance_id=15, substance_code= '7315' , substance_name= 'LSD' , substance_schedule=1, other_names= 'Acid, Mellow Yellow, Dots' , source_dataset=1, year=2022, parent_category=8),
        #print ("LSD inserted"),
        Substance(substance_id=16, substance_code= '7471' , substance_name= 'PCP' , substance_schedule=2, other_names= 'Unknown' , source_dataset=1, year=2022, parent_category=8),
        #print ("PCP inserted"),
        Substance(substance_id=17, substance_code= '7405' , substance_name= 'Ecstasy' , substance_schedule=1, other_names= 'Hug Drug, MDMA, Beans' , source_dataset=1, year=2022, parent_category=8),
        #print ("Ecstasy inserted"),
        Substance(substance_id=18, substance_code= '7285' , substance_name= 'Ketamine' , substance_schedule=3, other_names= 'Cat Valium, Jet K, Super K, Special K' , source_dataset=1, year=2022, parent_category=8),
        #print ("Ketamine inserted"),
        Substance(substance_id=19, substance_code= '7431/7432/Unknown' , substance_name= 'DMT/AMT/FOXY' , substance_schedule=1, other_names= 'Unknown' , source_dataset=1, year=2022, parent_category=8),
        #print ("DMT/AMT/FOXY inserted"),
        Substance(substance_id=20, substance_code= 'N/A' , substance_name= 'Salvia' , substance_schedule=-1, other_names= 'Maria Pastora, Sally-D' , source_dataset=1, year=2022, parent_category=8),
        #print ("Salvia inserted")
        Substance(substance_id=21, substance_code= '7415' , substance_name= 'Peyote' , substance_schedule=-1, other_names= 'Buttons, Cactus, Mesc' , source_dataset=1, year=2022, parent_category=8),
        Substance(substance_id=22, substance_code= '7381' , substance_name= 'Mescaline' , substance_schedule=-1, other_names= 'See Peyote' , source_dataset=1, year=2022, parent_category=8),
        Substance(substance_id=23, substance_code= '7437' , substance_name= 'Psilocybin' , substance_schedule=-1, other_names= 'Magic Mushrooms' , source_dataset=1, year=2022, parent_category=8)
    ]
        # Add more Substance instances as needed
     
    
    # Step 5: Add and commit the new rows
    session.add_all(substances_to_insert)
    session.commit()
    
    print("Data inserted successfully.")

except Exception as e:
    print(f"An error occurred: {e}")
    session.rollback()  # Rollback in case of any error

finally:
    # Step 6: Close the session
    session.close()
