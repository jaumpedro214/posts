# CREATE THE AWS GLUE JOB
# =======================

variable "enem-data-bucket-access-policy-arn" {
    type = string
}

# Create a new bucket to store the job script

resource "aws_s3_bucket" "enem-bucket-terraform-jobs" {
  bucket = "enem-bucket-terraform-jobs"
}

# UPLOAD THE SPARK JOB FILE myjob.py to s3

resource "aws_s3_object" "myjob" {
  bucket = aws_s3_bucket.enem-bucket-terraform-jobs.id
  key    = "myjob.py"
  source = "myjob.py"
}

# CREATE A NEW ROLE FOR THE GLUE JOB TO ASSUME
resource "aws_iam_role" "glue_execution_role" {
  name = "glue_execution_role_terraform"
  assume_role_policy = jsonencode({
    # This is the policy document that allows the role to be assumed by Glue
    # other services cannot assume this role
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

# ATTACH THE BASIC GLUE EXECUTION POLICY TO THE ROLE glue_execution_role
resource "aws_iam_role_policy_attachment" "glue_basic_execution" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  role       = aws_iam_role.glue_execution_role.name
}

# ATTACH THE THE S3 ACCESS POLICY s3_access_policy TO THE ROLE glue_execution_role
resource "aws_iam_policy_attachment" "s3_access_attachment_glue" {
  name       = "s3_and_glue_execution_access_attachment"
  policy_arn = var.enem-data-bucket-access-policy-arn
  roles      = [aws_iam_role.glue_execution_role.name]
}


# CREATE THE GLUE JOB
resource "aws_glue_job" "myjob" {
  name     = "myjob"
  role_arn = aws_iam_role.glue_execution_role.arn
  glue_version = "4.0"
  command {
    script_location = "s3://${aws_s3_bucket.enem-bucket-terraform-jobs.id}/myjob.py"
  }
  default_arguments = {
    "--job-language" = "python"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--enable-metrics" = ""
  }
  depends_on = [aws_s3_object.myjob]
}