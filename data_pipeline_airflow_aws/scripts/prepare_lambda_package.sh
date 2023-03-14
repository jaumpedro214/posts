cp lambda_function.py pdfextractor/lib64/python*/site-packages/
cd pdfextractor/lib/python*/site-packages/
zip -r lambda_function.zip *
mv lambda_function.zip ../../../../