import boto3
from PyPDF2 import PdfReader
import io
import json

def lambda_handler(event, context):
    object_key = event["Records"][0]["s3"]["object"]["key"]
    bucket = event["Records"][0]["s3"]["bucket"]["name"]

    object_uri = f"s3://{bucket}/{object_key}"

    if not object_uri.endswith(".pdf"):
        # Just to make sure that this function will not
        # cause a recursive loop
        return "Object is not a PDF"

    client = boto3.client("s3")

    try:
        pdf_file = client.get_object(Bucket=bucket, Key=object_key)
        pdf_file = io.BytesIO(pdf_file["Body"].read())
    except Exception as e:
        print(e)
        print(f"Error. Lambda was not able to get object from bucket {bucket}")
        raise e

    try:
        pdf = PdfReader(pdf_file)
        text = ""
        for page in pdf.pages:
            text += page.extract_text()

    except Exception as e:
        print(e)
        print(f"Error. Lambda was not able to parse PDF {object_uri}")
        raise e

    try:

        text_object = {
            "content": text,
            "original_uri": object_uri
        }

        client.put_object(
            Body=json.dumps(text_object).encode("utf-8"),
            Bucket=bucket,
            Key=f"content/{object_key[:-4]}.json" ,
        )
    except Exception as e:
        print(e)
        print(f"Error. Lambda was not able to put object in bucket {bucket}")
        raise e
    

