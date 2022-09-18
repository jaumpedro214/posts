-- ksql http://ksqldb-server:8088

SET 'auto.offset.reset'='earliest';

-- ==========================================================================
-- ======================== BRONZE LAYER - Accidents ========================
-- ==========================================================================

-- Mongodb Source Connector -> Bronze Layer
CREATE SOURCE CONNECTOR accidents_bronze_source_connector WITH (
    'connector.class' = 'io.debezium.connector.mongodb.MongoDbConnector',
    'mongodb.hosts' = 'mongo:27017',
    'mongodb.name' = 'replica-set',
    'mongodb.authsource' = 'admin',
    'mongodb.user' = 'mongo',
    'mongodb.password' = 'mongo',
    'collection.whitelist' = 'accidents.accidents_bronze',

    'transforms' = 'unwrap',
    'transforms.unwrap.type' = 'io.debezium.connector.mongodb.transforms.ExtractNewDocumentState',
    'transforms.unwrap.drop.tombstones' = 'false',
    'transforms.unwrap.delete.handling.mode' = 'drop',
    'transforms.unwrap.operation.header' = 'true',
    
    'errors.tolerance' = 'all'
);

-- Show the connector
DESCRIBE CONNECTOR accidents_bronze_source_connector;

-- Bronze Layer stream
CREATE STREAM accidents_bronze_stream
(
     _ID                      VARCHAR(STRING)
    ,ID                       VARCHAR(STRING)          
    ,PESID                    VARCHAR(STRING) 
    ,DATA_INVERSA             VARCHAR(STRING) 
    ,DIA_SEMANA               VARCHAR(STRING) 
    ,HORARIO                  VARCHAR(STRING) 
    ,UF                       VARCHAR(STRING) 
    ,BR                       VARCHAR(STRING) 
    ,KM                       VARCHAR(STRING) 
    ,MUNICIPIO                VARCHAR(STRING) 
    ,CAUSA_PRINCIPAL          VARCHAR(STRING) 
    ,CAUSA_ACIDENTE           VARCHAR(STRING) 
    ,ORDEM_TIPO_ACIDENTE      VARCHAR(STRING) 
    ,TIPO_ACIDENTE            VARCHAR(STRING) 
    ,CLASSIFICACAO_ACIDENTE   VARCHAR(STRING) 
    ,FASE_DIA                 VARCHAR(STRING) 
    ,SENTIDO_VIA              VARCHAR(STRING) 
    ,CONDICAO_METEREOLOGICA   VARCHAR(STRING) 
    ,TIPO_PISTA               VARCHAR(STRING) 
    ,TRACADO_VIA              VARCHAR(STRING) 
    ,USO_SOLO                 VARCHAR(STRING) 
    ,ID_VEICULO               VARCHAR(STRING) 
    ,TIPO_VEICULO             VARCHAR(STRING) 
    ,MARCA                    VARCHAR(STRING) 
    ,ANO_FABRICACAO_VEICULO   VARCHAR(STRING) 
    ,TIPO_ENVOLVIDO           VARCHAR(STRING) 
    ,ESTADO_FISICO            VARCHAR(STRING) 
    ,IDADE                    VARCHAR(STRING) 
    ,SEXO                     VARCHAR(STRING) 
    ,ILESOS                   INTEGER         
    ,FERIDOS_LEVES            INTEGER         
    ,FERIDOS_GRAVES           INTEGER         
    ,MORTOS                   INTEGER         
    ,LATITUDE                 VARCHAR(STRING) 
    ,LONGITUDE                VARCHAR(STRING) 
    ,REGIONAL                 VARCHAR(STRING) 
    ,DELEGACIA                VARCHAR(STRING) 
    ,UOP                      VARCHAR(STRING) 
)
WITH (
    kafka_topic = 'replica-set.accidents.accidents_bronze',
    value_format = 'avro'
);


-- ==========================================================================
-- ======================== SILVER LAYER - Accidents ========================
-- ==========================================================================

-- Bronze to Silver transformation stream
CREATE OR REPLACE STREAM accidents_bronze_to_silver
WITH (
    value_format = 'avro'
) AS
SELECT
    _id as `_id`,
    REGEXP_EXTRACT(
        '^m|^f', 
        LCASE(sexo)
    )
    AS gender,

    CASE
        WHEN tipo_acidente='Atropelamento de Pessoa' THEN 'run-over'
        WHEN tipo_acidente='Atropelamento de Pedestre' THEN 'run-over'
        WHEN tipo_acidente='Colisão com objeto estático' THEN 'collision with object'
        WHEN tipo_acidente='Colisão com objeto fixo' THEN 'collision with object'
        WHEN tipo_acidente='Colisão com objeto móvel' THEN 'collision with object'
        WHEN tipo_acidente='Colisão com objeto em movimento' THEN 'collision with object'
        WHEN tipo_acidente='Tombamento' THEN 'tipping'
        WHEN tipo_acidente='Saída de leito carroçável' THEN 'road exit'
        WHEN tipo_acidente='Saída de pista' THEN 'road exit'
        WHEN tipo_acidente='Capotamento' THEN 'rollover'
        WHEN tipo_acidente='Incêndio' THEN 'fire'
        WHEN tipo_acidente='Derramamento de carga' THEN 'cargo spill'
        WHEN tipo_acidente='Atropelamento de Animal' THEN 'animal run-over'
        WHEN tipo_acidente='Colisão traseira' THEN 'rear collision'
        WHEN tipo_acidente='Colisão frontal' THEN 'front collision'
        WHEN tipo_acidente='Colisão lateral' THEN 'lateral collision'
        WHEN tipo_acidente='Colisão transversal' THEN 'cross collision'
        WHEN tipo_acidente='Engavetamento' THEN 'traffic jam'
        ELSE 'other'
    END AS accident_type,

    ilesos AS unhurt,
    feridos_leves AS lightly_injured,
    feridos_graves AS strongly_injured,
    mortos AS dead,

    -- Check all possible data formats
    CASE
        WHEN LEN(REGEXP_EXTRACT('[0-9]{2}/[0-9]{2}/[0-9]{4}', data_inversa))>0 
        THEN PARSE_DATE(data_inversa, 'dd''/''MM''/''yyyy')
        WHEN LEN(REGEXP_EXTRACT('[0-9]{2}/[0-9]{2}/[0-9]{2}', data_inversa))>0 
        THEN PARSE_DATE(data_inversa, 'dd''/''MM''/''yy')
        WHEN LEN(REGEXP_EXTRACT('[0-9]{4}-[0-9]{2}-[0-9]{2}', data_inversa))>0 
        THEN PARSE_DATE(data_inversa, 'yyyy-MM-dd')
        ELSE NULL
    END AS `date`

FROM
accidents_bronze_stream;

-- Mongo Sink connector -> Silver Layer
CREATE SINK CONNECTOR ACCIDENTS_SILVER_SINK_CONNECTOR WITH (
    'connector.class'='com.mongodb.kafka.connect.MongoSinkConnector',
    'tasks.max'='1',

    'connection.uri'='mongodb://mongo:mongo@mongo:27017',
    'database'='accidents',
    'collection'='accidents_silver',
    'topics'='ACCIDENTS_BRONZE_TO_SILVER'
);


-- ==========================================================================
-- ========================  GOLD LAYER - Accidents  ========================
-- ==========================================================================

-- Silver to Gold transformation stream : Monthly aggregation
CREATE OR REPLACE TABLE gold_table_monthly_aggregated
WITH (
    value_format = 'avro'
) AS
SELECT 
    FORMAT_DATE(`date`, 'yyyy-MM') AS `_id`,

    -- Absolute numbers
    COUNT(*) AS total_accidents,
    SUM(unhurt) AS total_unhurt,
    SUM(lightly_injured) AS total_lightly_injured,
    SUM(strongly_injured) AS total_strongly_injured,
    SUM(dead) AS total_dead,

    -- percentual numbers
    AVG(unhurt) * 100 AS percentual_unhurt,
    AVG(lightly_injured) * 100 AS percentual_lightly_injured,
    AVG(strongly_injured) * 100 AS percentual_strongly_injured,
    AVG(dead) * 100 AS percentual_dead,

    -- gender 
    AVG(
        CASE
            WHEN gender='m' THEN 1
            ELSE 0
        END
    ) AS percentual_male_gender,

    AVG(
        CASE
            WHEN gender='f' THEN 1
            ELSE 0
        END
    ) AS percentual_female_gender
FROM
    accidents_bronze_to_silver
GROUP BY FORMAT_DATE(`date`, 'yyyy-MM')
EMIT CHANGES;


-- Mongo Sink connector -> Gold Layer : Monthly aggregation
CREATE SINK CONNECTOR ACCIDENTS_GOLD_SINK WITH (
    'topics'='GOLD_TABLE_MONTHLY_AGGREGATED', 
    
    'connector.class'='com.mongodb.kafka.connect.MongoSinkConnector',
    'tasks.max'='1',

    'connection.uri'='mongodb://mongo:mongo@mongo:27017',
    'database'='accidents',
    'collection'='accidents_gold',
    
    'transforms'='WrapKey',
    'transforms.WrapKey.type'='org.apache.kafka.connect.transforms.HoistField$Key',
    'transforms.WrapKey.field'='_id',

    'document.id.strategy'='com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy',
    'document.id.strategy.overwrite.existing'='true'
);


-- Silver to Gold transformation stream : Accident type aggregation
CREATE OR REPLACE TABLE gold_table_death_rates
WITH (
    value_format = 'avro'
) AS
SELECT  
    accident_type as `_id`,
    AVG(dead)   AS death_rate
FROM accidents_bronze_to_silver
GROUP BY accident_type
EMIT CHANGES;


-- Mongo Sink connector -> Gold Layer : Accident type aggregation
CREATE SINK CONNECTOR ACCIDENTS_DEATH_RATE_SINK WITH (
    'topics'='GOLD_TABLE_DEATH_RATES',
    
    'connector.class'='com.mongodb.kafka.connect.MongoSinkConnector',
    'tasks.max'='1',

    'connection.uri'='mongodb://mongo:mongo@mongo:27017',
    'database'='accidents',
    'collection'='death_rate_by_accident_type',
    
    'transforms'='WrapKey',
    'transforms.WrapKey.type'='org.apache.kafka.connect.transforms.HoistField$Key',
    'transforms.WrapKey.field'='_id',

    'document.id.strategy'='com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy',
    'document.id.strategy.overwrite.existing'='true'
);