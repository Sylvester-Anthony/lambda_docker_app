# Use the official AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.10

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the function code
COPY lambda_function.py ./

# Set the CMD to your handler (could also be done as a parameter override outside of Docker)
CMD ["lambda_function.lambda_handler"]