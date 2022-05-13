import requests
import unittest

BASE_URL = "http://127.0.0.1:8000/api/"


class TestAPICreateRegex(unittest.TestCase):
    def test_success_correct_parameters(self):
        data = {"name": "TheNameOfMyRegex",
                "pattern": r"(\W)"}
        response = requests.post(f"{BASE_URL}regex/", json=data)

        self.assertEqual(response.status_code, requests.codes.created)

    def test_fail_incorrect_parameters(self):
        data = {"name": None,
                "pattern": r"(\W)"}
        response = requests.post(f"{BASE_URL}regex/", json=data)

        self.assertEqual(response.status_code, requests.codes.bad_request)


class TestAPIDeleteRegex(unittest.TestCase):
    def test_sucess_delete(self):
        response = requests.delete(f"{BASE_URL}regex/TheNameOfMyRegex/")

        self.assertEqual(response.status_code, requests.codes.no_content)

    def test_fail_delete(self):
        response = requests.delete(f"{BASE_URL}regex/ThisEntryDontExist/")

        self.assertEqual(response.status_code, requests.codes.not_found)


class TestAPIProcessText(unittest.TestCase):
    def test_empty_steps(self):
        data = {
            "text": "This is my test text",
            "steps": []
        }
        expected_response = {
            "text": "This is my test text",
            "processed_text": "This is my test text",
        }
        response = requests.get(f"{BASE_URL}process/", json=data)

        self.assertEqual(expected_response, response.json())
    
    def test_success_replace(self):
        regex_data = {
            "name":"myregex",
            "pattern":"te",
            "replace_token":"A"
        }
        requests.post(f"{BASE_URL}regex/", json=regex_data)
        data = {
            "text": "This is my test text",
            "steps": ["myregex"]
        }
        expected_response = {
            "text": "This is my test text",
            "processed_text": "This is my Ast Axt",
        }
        response = requests.get(f"{BASE_URL}process/", json=data)

        self.assertEqual(expected_response, response.json())
