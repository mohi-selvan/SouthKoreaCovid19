import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "data225database", table_name = "patientinfo_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "data225database", table_name = "patientinfo_csv", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("patient_id", "long", "patient_id", "long"), ("code", "string", "code", "string"), ("sex", "string", "sex", "string"), ("age", "string", "age", "string"), ("country", "string", "country", "string"), ("infection_case", "string", "infection_case", "string"), ("infected_by", "long", "infected_by", "long"), ("contact_number", "long", "contact_number", "long"), ("symptom_onset_date", "string", "symptom_onset_date", "string"), ("confirmed_date", "string", "confirmed_date", "string"), ("released_date", "string", "released_date", "string"), ("deceased_date", "string", "deceased_date", "string"), ("state", "string", "state", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("patient_id", "long", "patient_id", "long"), ("code", "string", "code", "string"), ("sex", "string", "sex", "string"), ("age", "string", "age", "string"), ("country", "string", "country", "string"), ("infection_case", "string", "infection_case", "string"), ("infected_by", "long", "infected_by", "long"), ("contact_number", "long", "contact_number", "long"), ("symptom_onset_date", "string", "symptom_onset_date", "string"), ("confirmed_date", "string", "confirmed_date", "string"), ("released_date", "string", "released_date", "string"), ("deceased_date", "string", "deceased_date", "string"), ("state", "string", "state", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://bucket225s3/Patientinfo_parq/"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": "s3://bucket225s3/Patientinfo_parq/"}, format = "parquet", transformation_ctx = "datasink4")
job.commit()