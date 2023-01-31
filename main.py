from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import requests

urlAPI = 'https://api.bigdatacloud.net/data/reverse-geocode-client'
urlDB  = 'jdbc:postgresql://localhost:5432/spark'
driverDB = 'org.postgresql.Driver'
table = 'getAPILocation'
user = 'postgres'
password = ''

spark = SparkSession.builder.appName('GetAPI').getOrCreate()

spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

getData = requests.get(urlAPI).text

rdd = spark.sparkContext.parallelize([getData])

df = spark.read.json(rdd)

df_adm = df.select(
    'city',
    'continent',
    'continentCode',
    'countryCode',
    'countryName',
    'latitude',
    'locality',
    F.col('localityInfo.administrative'),
    'localityLanguageRequested',
    'longitude',
    'lookupSource',
    'plusCode',
    'postcode',
    'principalSubdivision',
    'principalSubdivisionCode',
)

df_inf = df.select(
    'city',
    'continent',
    'continentCode',
    'countryCode',
    'countryName',
    'latitude',
    'locality',
    F.col('localityInfo.informative'),
    'localityLanguageRequested',
    'longitude',
    'lookupSource',
    'plusCode',
    'postcode',
    'principalSubdivision',
    'principalSubdivisionCode',
)

df_adm = df_adm.select(
    'city',
    'continent',
    'continentCode',
    'countryCode',
    'countryName',
    'latitude',
    'locality',
    F.explode('administrative'),
    'localityLanguageRequested',
    'longitude',
    'lookupSource',
    'plusCode',
    'postcode',
    'principalSubdivision',
    'principalSubdivisionCode',
)

df_inf = df_inf.select(
    'city',
    'continent',
    'continentCode',
    'countryCode',
    'countryName',
    'latitude',
    'locality',
    F.explode('informative'),
    'localityLanguageRequested',
    'longitude',
    'lookupSource',
    'plusCode',
    'postcode',
    'principalSubdivision',
    'principalSubdivisionCode',
)

df_adm = df_adm.select(
    'city',
    'continent',
    'continentCode',
    'countryCode',
    'countryName',
    'latitude',
    'locality',
    F.col('col.*'),
    'localityLanguageRequested',
    'longitude',
    'lookupSource',
    'plusCode',
    'postcode',
    'principalSubdivision',
    'principalSubdivisionCode',
)

df_inf = df_inf.select(
    'city',
    'continent',
    'continentCode',
    'countryCode',
    'countryName',
    'latitude',
    'locality',
    F.col('col.*'),
    'localityLanguageRequested',
    'longitude',
    'lookupSource',
    'plusCode',
    'postcode',
    'principalSubdivision',
    'principalSubdivisionCode',
)

df_adm = df_adm.withColumn('localityInfoType', F.lit('administrative')).withColumn('postcode', F.lit(None).cast('string'))
df_inf = (df_inf.withColumn('localityInfoType', F.lit('informative'))
          .withColumn('postcode', F.lit(None).cast('string'))
          .withColumn('adminLevel', F.lit(None).cast('long'))
          .select('city',
                  'continent',
                  'continentCode',
                  'countryCode',
                  'countryName',
                  'latitude',
                  'locality',
                  'adminLevel',
                  'description',
                  'geonameId',
                  'isoCode',
                  'name',
                  'order',
                  'wikidataId',
                  'localityLanguageRequested',
                  'longitude',
                  'lookupSource',
                  'plusCode',
                  'postcode',
                  'principalSubdivision',
                  'principalSubdivisionCode',
                  'localityInfoType'
                 )
         )

df = df_adm.unionByName(df_inf)

df.write.jdbc(urlDB, table, mode='append', properties={'driver': driverDB, 'user': user, 'password': password})
