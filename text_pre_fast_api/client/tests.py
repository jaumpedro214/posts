import requests
from pprint import pprint

BASE_URL = "http://127.0.0.1:8000"

def test_request_no_params_error():
    response = requests.get( f"{BASE_URL}/preprocess/" )
    print("\nTest request with no params")
    print(response)
    pprint(response.json())

def test_request_params_sucess_no_steps():
    data = {"text":"This is my text", "steps":[]}
    response = requests.get( f"{BASE_URL}/preprocess/", json=data )
    print("\nTest request sucess no steps")
    print(response)
    pprint(response.json())

def test_preprocessing_example01():
    data = {
        "text":"Hello, my name is Joao, I'm 21. My phone Number is +55 99922-0214", 
        "steps":["replace_phone"]
    }
    response = requests.get( f"{BASE_URL}/preprocess/", json=data )
    print("\nExamples")
    print(response)
    pprint(response.json())

def test_preprocessing_example02():
    data = {
        "text":"""
               Travel Package No. 0214-66
               Vacancy on Brazil with our Tourism Agency.
               Individual Package Prices: U$ 1.999,99 (~R$ 6.999,99)
               
               For more information, contact us via:
               e-mail: regex@tourism.com.br or regex@tourism.contact.com.us
               phone: 1234-5678
               """, 
        "steps":["replace_phone", "replace_money", "replace_email", "replace_number_like", "lower"]
    }
    response = requests.get( f"{BASE_URL}/preprocess/", json=data )
    print("\nExamples")
    print(response)
    pprint(response.json())

if __name__ == "__main__":
    test_request_no_params_error()
    test_request_params_sucess_no_steps()
    test_preprocessing_example01()
    test_preprocessing_example02()