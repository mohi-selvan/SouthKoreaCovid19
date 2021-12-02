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
## @args: [database = "228gluedatabase", table_name = "fires", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "228gluedatabase", table_name = "fires", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("objectid", "string", "objectid", "string"), ("fod_id", "long", "fod_id", "long"), ("fpa_id", "string", "fpa_id", "string"), ("source_system_type", "string", "source_system_type", "string"), ("source_system", "string", "source_system", "string"), ("nwcg_reporting_agency", "string", "nwcg_reporting_agency", "string"), ("nwcg_reporting_unit_id", "string", "nwcg_reporting_unit_id", "string"), ("nwcg_reporting_unit_name", "string", "nwcg_reporting_unit_name", "string"), ("source_reporting_unit", "long", "source_reporting_unit", "long"), ("source_reporting_unit_name", "string", "source_reporting_unit_name", "string"), ("local_fire_report_id", "long", "local_fire_report_id", "long"), ("local_incident_id", "string", "local_incident_id", "string"), ("fire_code", "string", "fire_code", "string"), ("fire_name", "string", "fire_name", "string"), ("ics_209_incident_number", "string", "ics_209_incident_number", "string"), ("ics_209_name", "string", "ics_209_name", "string"), ("mtbs_id", "string", "mtbs_id", "string"), ("mtbs_fire_name", "string", "mtbs_fire_name", "string"), ("complex_name", "string", "complex_name", "string"), ("fire_year", "long", "fire_year", "long"), ("discovery_date", "double", "discovery_date", "double"), ("discovery_doy", "long", "discovery_doy", "long"), ("discovery_time", "long", "discovery_time", "long"), ("stat_cause_code", "double", "stat_cause_code", "double"), ("stat_cause_descr", "string", "stat_cause_descr", "string"), ("cont_date", "double", "cont_date", "double"), ("cont_doy", "long", "cont_doy", "long"), ("cont_time", "long", "cont_time", "long"), ("fire_size", "double", "fire_size", "double"), ("fire_size_class", "string", "fire_size_class", "string"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double"), ("owner_code", "double", "owner_code", "double"), ("owner_descr", "string", "owner_descr", "string"), ("state", "string", "state", "string"), ("county", "long", "county", "long"), ("fips_code", "long", "fips_code", "long"), ("fips_name", "string", "fips_name", "string"), ("shape", "string", "shape", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("objectid", "string", "objectid", "string"), ("fod_id", "long", "fod_id", "long"), ("fpa_id", "string", "fpa_id", "string"), ("source_system_type", "string", "source_system_type", "string"), ("source_system", "string", "source_system", "string"), ("nwcg_reporting_agency", "string", "nwcg_reporting_agency", "string"), ("nwcg_reporting_unit_id", "string", "nwcg_reporting_unit_id", "string"), ("nwcg_reporting_unit_name", "string", "nwcg_reporting_unit_name", "string"), ("source_reporting_unit", "long", "source_reporting_unit", "long"), ("source_reporting_unit_name", "string", "source_reporting_unit_name", "string"), ("local_fire_report_id", "long", "local_fire_report_id", "long"), ("local_incident_id", "string", "local_incident_id", "string"), ("fire_code", "string", "fire_code", "string"), ("fire_name", "string", "fire_name", "string"), ("ics_209_incident_number", "string", "ics_209_incident_number", "string"), ("ics_209_name", "string", "ics_209_name", "string"), ("mtbs_id", "string", "mtbs_id", "string"), ("mtbs_fire_name", "string", "mtbs_fire_name", "string"), ("complex_name", "string", "complex_name", "string"), ("fire_year", "long", "fire_year", "long"), ("discovery_date", "double", "discovery_date", "double"), ("discovery_doy", "long", "discovery_doy", "long"), ("discovery_time", "long", "discovery_time", "long"), ("stat_cause_code", "double", "stat_cause_code", "double"), ("stat_cause_descr", "string", "stat_cause_descr", "string"), ("cont_date", "double", "cont_date", "double"), ("cont_doy", "long", "cont_doy", "long"), ("cont_time", "long", "cont_time", "long"), ("fire_size", "double", "fire_size", "double"), ("fire_size_class", "string", "fire_size_class", "string"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double"), ("owner_code", "double", "owner_code", "double"), ("owner_descr", "string", "owner_descr", "string"), ("state", "string", "state", "string"), ("county", "long", "county", "long"), ("fips_code", "long", "fips_code", "long"), ("fips_name", "string", "fips_name", "string"), ("shape", "string", "shape", "string")], transformation_ctx = "applymapping1")
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
## @args: [connection_type = "s3", connection_options = {"path": "s3://bucket228s3/Fires/"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": "s3://bucket228s3/Fires/"}, format = "parquet", transformation_ctx = "datasink4")
job.commit()