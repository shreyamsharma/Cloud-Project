import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

# Define the pipeline options.
options = PipelineOptions(
    runner="DataflowRunner",
    project="finalproject-382823",
    region="northamerica-northeast1",
    temp_location="gs://project-dataset-finalproject-382823/temp",
    staging_location="gs://project-dataset-finalproject-382823/staging",
    service_account_email="dataflow-admin@finalproject-382823.iam.gserviceaccount.com",
    service_account_key="/home/shreyamanju1611/dashboard-project/finalproject-382823-1814c5e8b27b.json",
    setup_file="./setup.py"
)

# Define the schema for the CSV data.
schema = "id:STRING,width:FLOAT,height:FLOAT,initialFrame:INTEGER,finalFrame:INTEGER,numFrames:INTEGER,class:STRING,drivingDirection:STRING,traveledDistance:FLOAT,minXVelocity:FLOAT,maxXVelocity:FLOAT,meanXVelocity:FLOAT,numLaneChanges:INTEGER"

# Define the pipeline function.
def process_csv(row):
    if row.startswith('id'):
        return None

    # Split the CSV row into columns.
    columns = row.split(",")
    
    # Convert the columns to the appropriate data types.
    id = (columns[0])
    width = float(columns[1])
    height = float(columns[2])
    initial_frame = int(columns[3])
    final_frame = int(columns[4])
    num_frames = int(columns[5])
    class_ = columns[6]
    driving_direction = columns[7]
    traveled_distance = float(columns[8])
    min_x_velocity = float(columns[9])
    max_x_velocity = float(columns[10])
    mean_x_velocity = float(columns[11])
    num_lane_changes = int(columns[12])
    
    # Return a dictionary representing the processed row.
    return {
        "id": id,
        "width": width,
        "height": height,
        "initial_frame": initial_frame,
        "final_frame": final_frame,
        "num_frames": num_frames,
        "class": class_,
        "driving_direction": driving_direction,
        "traveled_distance": traveled_distance,
        "min_x_velocity": min_x_velocity,
        "max_x_velocity": max_x_velocity,
        "mean_x_velocity": mean_x_velocity,
        "num_lane_changes": num_lane_changes,
    }

# Define the pipeline.
with beam.Pipeline(options=options) as p:
    # Read the CSV file from Cloud Storage.
    lines = p | "ReadFromStorage" >> beam.io.ReadFromText("gs://project-dataset-finalproject-382823/01_tracksMeta.csv")
    
    # Process the CSV rows and convert them into Python objects.
    objects = lines | "ProcessCSV" >> beam.Map(process_csv)
    
    # Write the objects to BigQuery.
    objects | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
        "finalproject-382823.TracksOutput.tracksMeta",
        schema=schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    )
