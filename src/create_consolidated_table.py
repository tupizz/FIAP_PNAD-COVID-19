import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Set up Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'

# Initialize Spark Session with BigQuery support
spark = SparkSession.builder \
    .appName("BigQuery with PySpark") \
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2') \
    .config('spark.jars', './gcs-connector-hadoop2-latest.jar') \
    .getOrCreate()

sc = spark.sparkContext

# Set log level
sc.setLogLevel("INFO")
sc._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
sc._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.json.keyfile', os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

def read_bigquery_table(table_name):
    return spark.read.format('bigquery').option('table', table_name).load()

df1 = read_bigquery_table('fiap-417300.PNAD_COVID_19.05_2020')
df2 = read_bigquery_table('fiap-417300.PNAD_COVID_19.06_2020')
df3 = read_bigquery_table('fiap-417300.PNAD_COVID_19.07_2020')

# Get the column names of each dataframe
columns_df1 = set(df1.columns)
columns_df2 = set(df2.columns)
columns_df3 = set(df3.columns)

# Find the common columns between all three dataframes
common_columns = columns_df1.intersection(columns_df2).intersection(columns_df3)

# Select only the common columns from each dataframe
df1_aligned = df1.select(*common_columns)
df2_aligned = df2.select(*common_columns)
df3_aligned = df3.select(*common_columns)

# Now you can safely perform the union operation
combined_df = df1_aligned.union(df2_aligned).union(df3_aligned)

# Column Mapping
column_mapping = {
    'UF': 'estado',
    'A002': 'idade',
    'A003': 'genero',
    'A005': 'escolaridade',
    'B0011': 'quest_1', # Na semana passada teve febre?
    'B0013': 'quest_2', # Na semana passada teve dor de garganta?
    'B0016': 'quest_3', # Na semana passada teve dor no peito?
    'B00112': 'quest_4', # Na semana passada teve dor muscular?
    'B00110': 'quest_5', # Na semana passada teve dor nos olhos?
    'B0014': 'quest_6', # Na semana passada teve dificuldade para respirar?
    'B0031': 'quest_7', # Providência tomada para recuperar dos sintomas foi ficar em casa
    'B0032': 'quest_8', # Providência tomada para recuperar dos sintomas foi ligar para algum profissional de saúde
    'B0033': 'quest_9', # Providência tomada  para recuperar dos sintomas foi comprar e/ou tomar  remédio por conta própria
    'B0034': 'quest_10', # Providência tomada para recuperar dos sintomas foi comprar e/ou tomar remédio por orientação médica
    'B007': 'quest_11', # Tem algum plano de saúde médico, seja particular, de empresa ou de órgão público
    'C006': 'quest_12', # Tem mais de um trabalho
    'C014': 'quest_13', # O(A) Sr(a) contribui para o INSS?
    'C007D': 'work_activity', # Qual é a principal atividade do local ou empresa em que você trabalha?
}

transformed_df = combined_df
for old_name, new_name in column_mapping.items():
    transformed_df = transformed_df.withColumnRenamed(old_name, new_name)

# After renaming, update columns_to_keep to the new names
columns_to_keep = [column_mapping.get(col, col) for col in combined_df.columns if col in column_mapping or column_mapping.get(col)]
transformed_df = transformed_df.select(*columns_to_keep)

estado_mapping = {
    11: 'Rondônia',
    12: 'Acre',
    13: 'Amazonas',
    14: 'Roraima',
    15: 'Pará',
    16: 'Amapá',
    17: 'Tocantins',
    21: 'Maranhão',
    22: 'Piauí',
    23: 'Ceará',
    24: 'Rio Grande doNorte',
    25: 'Paraíba',
    26: 'Pernambuco',
    27: 'Alagoas',
    28: 'Sergipe',
    29: 'Bahia',
    31: 'Minas Gerais',
    32: 'Espírito Santo',
    33: 'Rio de Janeiro',
    35: 'São Paulo',
    41: 'Paraná',
    42: 'Santa Catarina',
    43: 'Rio Grande do Sul',
    50: 'Mato Grosso do Sul',
    51: 'Mato Grosso',
    52: 'Goiás',
    53: 'Distrito Federal'
}

for key, value in estado_mapping.items():
    transformed_df = transformed_df.withColumn('estado', when(col('estado') == key, value).otherwise(col('estado')))

sexo_mapping = { 1: 'Homem',2: 'Mulher' }

for key, value in sexo_mapping.items():
    transformed_df = transformed_df.withColumn('genero', when(col('genero') == key, value).otherwise(col('genero')))

quest_resposta_mapping = {1: 'Sim', 2: 'Não', 3: 'Não sabe', 9: 'Ignorado'}

for i in range(1, 14):
    for key, value in quest_resposta_mapping.items():
        transformed_df = transformed_df.withColumn(f'quest_{i}', when(col(f'quest_{i}') == key, value).otherwise(col(f'quest_{i}')))

escolaridade_mapping = {
    1: 'Sem instrução',
    2: 'Fundamental incompleto',
    3: 'Fundamental completa',
    4: 'Médio incompleto',
    5: 'Médio completo',
    6: 'Superior incompleto',
    7: 'Superior completo',
    8: 'Pós-graduação, mestrado ou doutorado'
}

for key, value in escolaridade_mapping.items():
    transformed_df = transformed_df.withColumn('escolaridade', when(col('escolaridade') == key, value).otherwise(col('escolaridade')))

activity_mapping = {
    1: "Agricultura, pecuária, produção florestal e pesca",
    2: "Extração de petróleo, carvão mineral, minerais metálicos, pedra, areia, sal etc.",
    3: "Indústria da transformação (inclusive confecção e fabricação caseira)",
    4: "Fornecimento de eletricidade e gás, água, esgoto e coleta de lixo",
    5: "Construção",
    6: "Comércio no atacado e varejo",
    7: "Reparação de veículos automotores e motocicletas",
    8: "Transporte de passageiros",
    9: "Transporte de mercadorias",
    10: "Armazenamento, correios e serviços de entregas",
    11: "Hospedagem (hotéis, pousadas etc.)",
    12: "Serviço de alimentação (bares, restaurantes, ambulantes de alimentação)",
    13: "Informação e comunicação (jornais, rádio e televisão, telecomunicações e informática)",
    14: "Bancos, atividades financeiras e de seguros",
    15: "Atividades imobiliárias",
    16: "Escritórios de advocacia, engenharia, publicidade e veterinária (Atividades profissionais, científicas e técnicas)",
    17: "Atividades de locação de mão de obra, segurança, limpeza, paisagismo e teleatendimento",
    18: "Administração pública (governo federal, estadual e municipal)",
    19: "Educação",
    20: "Saúde humana e assistência social",
    21: "Organizações religiosas, sindicatos e associações",
    22: "Atividade artísticas, esportivas e de recreação",
    23: "Cabeleireiros, tratamento de beleza e serviços pessoais",
    24: "Serviço doméstico remunerado (será imputado da posição na ocupação)",
    25: "Outro",
}

for key, value in activity_mapping.items():
    transformed_df = transformed_df.withColumn('work_activity', when(col('work_activity') == key, value).otherwise(col('work_activity')))


# Check the combined schema and data
print("Schema of combined_df:")
transformed_df.printSchema()

print("Sample data from combined_df:")
transformed_df.show(5)

# Upload to BigQuery
# Ensure you have the necessary permissions and that the dataset and table exist or are created automatically
transformed_df.write.format('bigquery') \
    .option('table', 'fiap-417300.PNAD_COVID_19_UNITED.2020_MAY_TO_JULY') \
    .option('temporaryGcsBucket','fiap-bucket-tadeu') \
    .mode('overwrite') \
    .save()

