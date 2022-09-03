-- ksql http://ksqldb-server:8088

SET 'auto.offset.reset'='earliest';


-- Mongodb Source Connector
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
    'transforms.unwrap.operation.header' = 'true'
);

-- Show the connector
DESCRIBE CONNECTOR accidents_bronze_source_connector;

-- 'errors.tolerance'='all',
-- 'errors.deadletterqueue.topic.name'='dlq_accidents_bronze',
-- 'errors.deadletterqueue.topic.replication.factor'=1

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

SELECT * FROM accidents_bronze_stream;

SELECT
    _id, 
    data_inversa, 
    ilesos, 
    feridos_leves, 
    feridos_graves, 
    mortos, 
    sexo, 
    tipo_acidente
FROM accidents_bronze_stream;

-- Create Bronze to Silver Stream

NULLIF(
        REGEXP_REPLACE(
            SUBSTRING(
                LCASE(sexo), 
                0, 
                1
            ), 
            '[^mf]', 
            'o'
        ),
        'o' 
    )

SELECT
    REGEXP_EXTRACT(
        '^m|^f', 
        LCASE(sexo)
    )
    AS sex,

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
    feridos_leves AS lighly_injured,
    feridos_graves AS strongly_injured,
    mortos AS dead,

    REGEXP_EXTRACT('[0-9]{4}', data_inversa)
    AS accident_year

FROM
accidents_bronze_stream;