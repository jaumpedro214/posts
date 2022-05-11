from fastapi import FastAPI

from models import PreprocessingRequest

from preprocessing import *

step_name_to_obj = {
    "remove_punkt":RegExRemovePunkt(),
    "replace_number_like":RegExReplaceNumberLike(),
    "replace_money":RegExReplaceMoney(),
    "replace_phone":RegExReplacePhone(),
    "replace_email":RegExReplaceEMail(),
    "lower":Lowercase()
}

app = FastAPI()

@app.get("/")   
def root():
    return {"Hello":"World!"}

@app.get("/preprocess/")
def preprocess( request: PreprocessingRequest ):
    ## Do preprocessing

    steps = request.steps
    text = str(request.text)
    preprocessed_text = ""+text

    for step in steps:
        preprocessed_text = step_name_to_obj[step].preprocess(preprocessed_text)

    response = { 
        "steps":steps,
        "text":text,
        "preprocessed_text":preprocessed_text
    }
    return response
