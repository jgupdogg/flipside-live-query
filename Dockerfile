FROM public.ecr.aws/lambda/python:3.8

# Copy function code
COPY lf_sol_network.py ${LAMBDA_TASK_ROOT}

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt -t ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (function.lambda_handler)
CMD ["lf_sol_network.lambda_handler"]
