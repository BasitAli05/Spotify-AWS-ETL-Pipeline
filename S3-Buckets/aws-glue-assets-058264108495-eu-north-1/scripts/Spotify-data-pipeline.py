import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Albums
Albums_node1724774819733 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-proj-bucket/staging/albums_data.csv"], "recurse": True}, transformation_ctx="Albums_node1724774819733")

# Script generated for node Artists
Artists_node1724774820021 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-proj-bucket/staging/artist_data.csv"], "recurse": True}, transformation_ctx="Artists_node1724774820021")

# Script generated for node Tracks
Tracks_node1724774820261 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-proj-bucket/staging/tracks_data.csv"], "recurse": True}, transformation_ctx="Tracks_node1724774820261")

# Script generated for node Join Album & Artist
JoinAlbumArtist_node1724774922855 = Join.apply(frame1=Albums_node1724774819733, frame2=Artists_node1724774820021, keys1=["artist_id"], keys2=["id"], transformation_ctx="JoinAlbumArtist_node1724774922855")

# Script generated for node Join with tracks
Joinwithtracks_node1724775060919 = Join.apply(frame1=Tracks_node1724774820261, frame2=JoinAlbumArtist_node1724774922855, keys1=["track_id"], keys2=["track_id"], transformation_ctx="Joinwithtracks_node1724775060919")

# Script generated for node Drop Fields
DropFields_node1724775224695 = DropFields.apply(frame=Joinwithtracks_node1724775060919, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1724775224695")

# Script generated for node Destination
Destination_node1724775239514 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1724775224695, connection_type="s3", format="glueparquet", connection_options={"path": "s3://spotify-proj-bucket/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1724775239514")

job.commit()