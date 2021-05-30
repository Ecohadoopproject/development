import pymongo
from pyspark.sql import SparkSession


def write_mongodb(id_enumerated,ecoregion_code,species_id,eco_endemic):
    myclient = pymongo.MongoClient("mongodb://127.0.0.1:27017")
    mydb = myclient["bigdata"]
    mycol = mydb["ecoregion_species"]
    array = {"ID":id_enumerated,"ECOREGION_CODE":ecoregion_code,"SPECIED_ID":species_id,"ECO_ENDEMIC":eco_endemic}
    mycol.insert_one(array)




spark = SparkSession.builder.getOrCreate()

data = spark.read.csv(
    "hdfs://172.18.0.2/user/maria_dev/bigdata/ecoregion_species.csv", 
    header=True
)

data.foreach(lambda x: write_mongodb(x["ID"],x["ECOREGION_CODE"],x["SPECIES_ID"],x["ECO_ENDEMIC"]), print("Transferred"))