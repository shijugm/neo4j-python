"""

Below is a complete command line for running the pipeline

Directrunner :
##############
python   neo4j_python/lab/df_neo.py\
    --temp_location gs://dataflow-shiju/neo4jtemp 

    

Dataflow Runner:  
#################

python neo4j_python/lab/df_neo.py\
  --job_name df-neo4j \
  --project shiju-sandbox \
  --region australia-southeast1 \
  --runner DataflowRunner \
  --temp_location gs://dataflow-shiju/neo4jtemp  \
  --setup_file ./setup.py \
  --service_account_email='dataflowrunner@shiju-sandbox.iam.gserviceaccount.com' 
 
"""
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from neo4j import GraphDatabase


class WriteToNeo4j(beam.DoFn):

    def __init__(self,   *args ):
        print("start init")
        self.args = args
        self.uri = args[0]
        self.username = args[1]
        self.password = args[2]
        self.driver = None

        print("driver initialized")

    def start_bundle(self):
        # Connect to the database at the start. Does not work for DirectRunner 
        print("In setup")
        self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        # pass

    # Close connection
    def close(self):
        self.driver.close()

    def process(self, element):
        print("start process")
        for dictionary in element:
            for key, value in dictionary.items():   
                print(key, ":", value)     
                # Comment out when Dataflow runner is used
                # self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
                self.driver.execute_query(f"""
                  CREATE (p:{key}) 
                  SET p.name = '{value}'
                  """ ,
                database_="neo4j"
                ) 
                # self.driver.close()

def run():  
    input_words = [
          [{'Package' : 'P1'}],
          [{'Package' : 'P2'}],
          [{'Package' : 'P3'}],
          [{'Depot' : 'D1'}],
          [{'Depot' : 'D2'}], 
        ]
    
    # neo_db_args =   "neo4j+s://9497a055.databases.neo4j.io:7687", "neo4j", "Ge2-kcyV7T65AvXdeiagiIfiS5W6VX268lSEa8gd4aA"
    
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()

    # Create the pipeline
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    # options.view_as(beam.options.pipeline_options.SetupOptions).requirements_file = 'requirements.txt'

    
    with beam.Pipeline(options=options) as p:
        # Create a PCollection from a list
        input_data = p | beam.Create(input_words)  
        processed_data = input_data | beam.ParDo(WriteToNeo4j("neo4j+s://9497a055.databases.neo4j.io", "neo4j", "Ge2-kcyV7T65AvXdeiagiIfiS5W6VX268lSEa8gd4aA"))

  

if __name__ == '__main__':
    run()


# import apache_beam as beam
# from apache_beam.options.pipeline_options import PipelineOptions

# def process_element(element):
#     for dictionary in element:
#         for key, value in dictionary.items():
#             print(key, ":", value)

# def run():
#     options = PipelineOptions()

#     with beam.Pipeline(options=options) as p:
#         # Example PCollection with multiple dictionaries
#         data = p | beam.Create([
#             [{'key1': 'value1', 'key2': 'value2'}, {'key3': 'value3'}],
#             [{'key4': 'value4'}, {'key5': 'value5', 'key6': 'value6'}]
#         ])

#         # Process each element
#         processed_data = data | beam.ParDo(process_element)

# if __name__ == '__main__':
#     run()