terraform {
  required_version = ">= 0.12"

  required_providers {
    aws = ">= 3.51.0"
  }

}

variable "AWS_ACCESS_KEY_ID" {
  type = string
}

variable "AWS_SECRET_ACCESS_KEY" {
  type = string
}

variable "AWS_DEFAULT_REGION" {
  type = string
}

provider "aws" {
  access_key = var.AWS_ACCESS_KEY_ID
  secret_key = var.AWS_SECRET_ACCESS_KEY
  region     = var.AWS_DEFAULT_REGION
}

# CREATE THE S3 BUCKET
# =====================

# resource <type> <name>  
# the name is used to refer to the resource. This is not the same as the name of the resource in AWS.
resource "aws_s3_bucket" "enem-data-bucket" {
  bucket = "enem-data-bucket"
}


# CREATE THE LAMBDA FUNCTION
# ==========================

# CREATE A NEW ROLE FOR THE LAMBDA FUNCTION TO ASSUME
resource "aws_iam_role" "lambda_execution_role" {
  name = "lambda_execution_role_terraform"
  assume_role_policy = jsonencode({
    # This is the policy document that allows the role to be assumed by Lambda
    # other services cannot assume this role
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}


# ATTACH THE BASIC LAMBDA EXECUTION POLICY TO THE ROLE lambda_execution_role
resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda_execution_role.name
}


# CREATE A NEW POLICY FOR THE LAMBDA FUNCTION TO ACCESS S3
resource "aws_iam_policy" "s3_access_policy" {
  name = "s3_access_policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = aws_s3_bucket.enem-data-bucket.arn
      }
    ]
  })
}


# ATTACH THE EXECUTION POLICY AND THE S3 ACCESS POLICY TO THE ROLE lambda_execution_role
resource "aws_iam_policy_attachment" "s3_access_attachment" {
  name       = "s3_and_lambda_execution_access_attachment"
  policy_arn = aws_iam_policy.s3_access_policy.arn
  roles      = [aws_iam_role.lambda_execution_role.name]
}


# CREATE A NEW LAMBDA FUNCTION
resource "aws_lambda_function" "lambda_function" {
  function_name = "my-lambda-function-aws-terraform-jp"
  role          = aws_iam_role.lambda_execution_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.8"
  filename      = "lambda_function.zip"
}

# ADD A TRIGGER TO THE LAMBDA FUNCTION BASED ON S3 BUCKET CREATION EVENTS
# https://stackoverflow.com/questions/68245765/add-trigger-to-aws-lambda-functions-via-terraform

resource "aws_lambda_permission" "allow_bucket_execution" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_function.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.enem-data-bucket.arn
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.enem-data-bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_function.arn
    events              = ["s3:ObjectCreated:*"]
    # filter_prefix       = "processed/"
    filter_suffix = ".pdf"
  }

  depends_on = [aws_lambda_permission.allow_bucket_execution]
}


module "glue" {
  source = "./glue"
  enem-data-bucket-access-policy-arn = aws_iam_policy.s3_access_policy.arn
}