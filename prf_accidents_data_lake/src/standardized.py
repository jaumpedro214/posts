from connectors import MongoDBConnector
import re

from datetime import datetime

# CONSTANTS & CONFIGURATIONS
MISSING_VALUE_TOKEN = None

def regex_replacer(text:str, rules:dict) -> str:
    """
    Perform several regex replace operations in a text.

    Parameters
    ----------
    text : str
        The text to be processed
    rules : dict
        dict containing the replacing rules.
        Each entry represents a pair pattern:replace_by. 

    Returns
    -------
    str
        processed text
    """
    if text==None:
        return None
    
    for pattern, replace_token in rules.items():
        text = re.sub(pattern, replace_token, text)
    return text    

def normalize_text(text):
    """
    Remove trailing spaces, put string in lowercase and
    replace null values with a constant value.

    Parameters
    ----------
    text : text
        The text to be processed

    Returns
    -------
    text
        The processed text
    """
    text = str(text).strip().lower()
    if text in ["(null)", "nan", "none"]:
        return MISSING_VALUE_TOKEN
    return text

# TRANSFORMERS FOR EACH CATHEGORICAL FIELD
# ----------------------------------------

def transform_sexo(sexo):
    sexo = str(sexo).lower()
    sexo = sexo[0] # M, F or I
    
    if sexo != "m" and sexo != "f":
        sexo = MISSING_VALUE_TOKEN
    return sexo

def transform_estado_fisico(estado_fisico):
    # Remove trailing spaces
    estado_fisico = normalize_text(estado_fisico)
    
    if estado_fisico in ["(null)", "nan", "none"]:
        return MISSING_VALUE_TOKEN
    
    estado_fisico = regex_replacer(
        estado_fisico,
        {
            "lesões":"ferido",
            "graves":"grave",
            "leves":"leve",
            "óbito":"morto"
        }
    )
    
    return estado_fisico

def transform_causa_acidente(causa_acidente):
    
    if causa_acidente==MISSING_VALUE_TOKEN:
        return MISSING_VALUE_TOKEN
    
    causa_acidente = normalize_text(causa_acidente)
    
    causa_acidente = regex_replacer(
        causa_acidente,
        {
            "demais fenômenos da natureza":"fenômenos da natureza",
            "chuva":"fenômenos da natureza",
            
            "demais falhas mecânicas ou elétricas":"falha mecânica", 
            "defeito mecânico no veículo":"falha mecânica",
            "problema com o freio":"falha mecânica",
            "avarias e/ou desgaste excessivo no pneu":"falha mecânica",
            
            "não guardar distância de segurança":"não guardar distância de segurança", 
            "condutor deixou de manter distância do veículo da frente":"não guardar distância de segurança",

            "pedestre andava na pista":"pedestre fora da faixa",
            "pedestre cruzava a pista fora da faixa":"pedestre fora da faixa",
            "entrada inopinada do pedestre":"pedestre fora da faixa",
            
            "ingestão de substâncias psicoativas pelo condutor":"ingestão de substâncias psicoativas",
            
            "mal súbito":"mal súbito do condutor",
            
            "ingestão de álcool ou de substâncias psicoativas pelo pedestre":"ingestão de álcool e/ou substâncias psicoativas pelo pedestre",
        
            "demais falhas na via":"defeito na via",
            
            "sinalização da via insuficiente ou inadequada":"deficiência do sistema de iluminação/sinalização",
            "ausência de sinalização":"deficiência do sistema de iluminação/sinalização",

            "ausência de reação do condutor":"reação tardia ou ineficiente do condutor",
            
            "falta de atenção à condução":"falta de atenção"
        }
    )
    
    return causa_acidente

def transform_tipo_acidente(tipo_acidente):
    if tipo_acidente==MISSING_VALUE_TOKEN:
        return MISSING_VALUE_TOKEN
    tipo_acidente = normalize_text(tipo_acidente)
    
    tipo_acidente = regex_replacer(
        tipo_acidente,
        {
            "atropelamento de pessoa":"atropelamento",
            "atropelamento de pedestre":"atropelamento",
            
            "colisão com objeto estático":"colisão com objeto",
            "colisão com objeto fixo":"colisão com objeto",
            "colisão com objeto móvel":"colisão com objeto",
            "colisão com objeto em movimento":"colisão com objeto",
            
            "danos eventuais":"eventos atípicos",
            
            "saída de leito carroçável":"saída de pista",
        }
    )
    
    return tipo_acidente
    
def transform_classificacao_acidente(classificacao_acidente):
    return normalize_text(classificacao_acidente)
    
def transform_fase_dia(fase_dia):
    return normalize_text(fase_dia)

def transform_tipo_pista(tipo_pista):
    return normalize_text(tipo_pista)

def transform_tracado_via(tracado_via):
    return normalize_text(tracado_via)

def transform_uso_solo(uso_solo):
    return normalize_text(uso_solo)

def transform_tipo_veiculo(tipo_veiculo):
    tipo_veiculo = normalize_text(tipo_veiculo)
    
    if tipo_veiculo == MISSING_VALUE_TOKEN:
        return MISSING_VALUE_TOKEN
    
    tipo_veiculo = regex_replacer(
        tipo_veiculo,
        {
            "-":"",
        }
    )
    
    if tipo_veiculo[-1] == "s":
        tipo_veiculo = tipo_veiculo[:-1]
    
    return tipo_veiculo

def transform_condicao_metereologica(condicao_metereologica):
    condicao_metereologica = normalize_text(condicao_metereologica)
    if condicao_metereologica == MISSING_VALUE_TOKEN:
        return MISSING_VALUE_TOKEN
    
    condicao_metereologica = regex_replacer(
        condicao_metereologica,
        {
            "céu":"ceu",
            "nevoeiro/neblina":"nublado",
            "ignorada":"ignorado"
        }
    )
    
    return condicao_metereologica

def transform_sentido_via(sentido_via):
    return normalize_text(sentido_via)

def transform_tipo_envolvido(tipo_envolvido):
    return normalize_text(tipo_envolvido)

def transform_dia_semana(dia_semana):
    dia_semana = normalize_text(dia_semana)
    if dia_semana == MISSING_VALUE_TOKEN:
        return MISSING_VALUE_TOKEN
    
    dia_semana = regex_replacer(
        dia_semana,
        {
            "-feira":""
        }
    )
    
    return dia_semana

# TRANSFORMERS FOR OTHER FIELDS
# -----------------------------

def transform_ids(id, pesid):
    id = normalize_text(id)
    pesid = normalize_text(pesid)
    
    if MISSING_VALUE_TOKEN in [id, pesid]:
        return MISSING_VALUE_TOKEN
    
    id = id.replace('.', '')
    pesid = pesid.replace('.', '')
    
    final_id = f"{id}:{pesid}"
    return final_id

def transform_data(data_inversa, horario):
    data_inversa = normalize_text(data_inversa)
    horario = normalize_text(horario)
    
    if MISSING_VALUE_TOKEN in [horario, data_inversa]:
        return MISSING_VALUE_TOKEN
    
    data = f"{data_inversa} {horario}"
    data_fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S"
    ]
    
    for fmt in data_fmts:
        try:
            return datetime.strptime(data, fmt)
        except ValueError:
            pass
    return MISSING_VALUE_TOKEN
    
def transform_br(br):
    if br == MISSING_VALUE_TOKEN:
        return MISSING_VALUE_TOKEN
    
    try:
        br = f'{int(float(br)):03}' # convert to int
    except:
        pass
    
    return normalize_text(br)

def transform_km(km):
    km = normalize_text(km)
    km = regex_replacer(km,{",":"."})
    try:
        return float(km)
    except:
        return MISSING_VALUE_TOKEN

def transform_idade(idade):
    idade = normalize_text(idade)
    try:
        return int(idade)
    except:
        return MISSING_VALUE_TOKEN

def transform_marca(marca):
    return normalize_text(marca)

def transform_id_veiculo(id_veiculo):
    id_veiculo = normalize_text(id_veiculo)
    
    if id_veiculo == MISSING_VALUE_TOKEN:
        return MISSING_VALUE_TOKEN
    
    id_veiculo = id_veiculo.replace('.', '')
    return id_veiculo

# Dict mapping to the transformations
# 
# final-field-name : ((field-01, field-02, ...), transformation_function)
# The new field will be created as: 
#   newdocument[final-field-name] = transformation_function(field-01, field-02, ...)
# 
# Structure
# ---------
# final-field-name:
#   Field name in the processed database.
# (field-01, field-02, ...): 
#   Fields from the original(raw) database to be included as args in the transformation function
# transformation_function: 
#   Function to perform the transformation and create the new field

PARAMETERS=0
FUNCTION=1
TRANSFORM_DICT ={
    'id':(
        ('id', 'pesid'),transform_ids
    ), 
    'data':(
        ('data_inversa', 'horario'),transform_data
    ),
    'dia_semana':(
        ('dia_semana',),transform_dia_semana
    ), 
    'uf':(
        ('uf',),normalize_text
    ), 
    'br':(
        ('br',),transform_br
    ), 
    'km':(
        ('km',),transform_km
    ),
    'municipio':(
        ('municipio',),normalize_text
    ), 
    'causa_acidente':(
        ('causa_acidente',),transform_causa_acidente
    ), 
    'tipo_acidente':(
        ('tipo_acidente',),transform_tipo_acidente
    ),
    'classificacao_acidente':(
        ('classificacao_acidente',),transform_classificacao_acidente
    ), 
    'fase_dia':(
        ('fase_dia',),transform_fase_dia
    ),
    'sentido_via':(
        ('sentido_via',),transform_sentido_via
    ), 
    'condicao_metereologica':(
        ('condicao_metereologica',),transform_condicao_metereologica
    ),
    'tipo_pista':(
        ('tipo_pista',),transform_tipo_pista
    ), 
    'tracado_via':(
        ('tracado_via',),transform_tracado_via
    ), 
    'uso_solo':(
        ('uso_solo',),transform_uso_solo
    ),
    'id_veiculo':(
        ('id_veiculo',),transform_id_veiculo
    ), 
    'tipo_veiculo':(
        ('tipo_veiculo',),transform_tipo_veiculo), 
    'marca':(
        ('marca',),transform_marca
    ), 
    'tipo_envolvido':(
        ('tipo_envolvido',),transform_tipo_envolvido
    ), 
    'estado_fisico':(
        ('estado_fisico',),transform_estado_fisico
    ), 
    'idade':(
        ('idade',),transform_idade
    ),
    'sexo':(
        ('sexo',),transform_sexo
    ),
}

class PRFDataTransformer:
    def __init__(self, batch_size=50000) -> None:
        """
        Reads data from a database, perform the needed
        transformations and stores the results.

        Parameters
        ----------
        batch_size : int, optional
            Number of entries processed at once, used to avoid memory issues, by default 50000
        """
        self.db_connector = MongoDBConnector(database="PRFAcidentes")
        self.batch_size = batch_size

    def transform_document(self, document: dict) -> dict:
        """
        Recieves a document and perform the needed transformations.
        Is based on the TRANSFORM_DICT defined outside the class scope.

        Parameters
        ----------
        document : dict
            JSON-like object representing the record to be processed

        Returns
        -------
        dict
            JSON-like object representing the processed record
        """
        
        new_document = dict()
        for fieldname, transform_rule in TRANSFORM_DICT.items():
            new_document[fieldname] = transform_rule[FUNCTION](
                *tuple(document[key] for key in transform_rule[PARAMETERS])
            )
        return new_document

    def transform(self, from_collection='raw', to_collection='processed'):
        """
        Read all documents from a collection to perform the needed transformations.
        Stores the results in another collection.

        Parameters
        ----------
        from_collection : str, optional
            String name of the internal database structure where
            the data is stored (Mongodb collection, PostgreSQL table, etc...).
            By default 'raw'
        to_collection : str, optional
            String name of the internal database structure where the data will be saved.
            By default 'processed'
        """
        
        db_cursor = self.db_connector.find(collection=from_collection)
        print(f"[{datetime.now()}] Transforming objects.")

        count = 0
        transformed_documents = []
        for document in db_cursor:
            transformed_documents.append(
                self.transform_document(document)
            )
            count += 1
            
            if count%self.batch_size==0 and count!=0:
                # When the number of processed documents reach the BATCH_SIZE:
                # 1 - Store the processed documents ino the database
                # 2 - Erase them from memory
                
                print(f"[{datetime.now()}] {count} processed. Inserting into database.")
                
                self.db_connector.insert_many(
                    entries=transformed_documents,
                    collection=to_collection
                )
                
                del transformed_documents
                transformed_documents = []
                
                print(f"[{datetime.now()}] finished.")
        
        # Insert the remaining documents
        if len(transformed_documents) > 0:
            print(f"[{datetime.now()}] {count} processed. Inserting into database.")
            
            self.db_connector.insert_many(
                entries=transformed_documents,
                collection=to_collection
            )
            
            print(f"[{datetime.now()}] finished.")


if __name__=="__main__":
    
    prf_data_transformer = PRFDataTransformer()
    prf_data_transformer.transform()