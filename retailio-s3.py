import os
import boto3
import pandas as pd
import awswrangler as wr
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get AWS credentials and region from environment variables
ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
REGION = os.getenv('AWS_REGION')

print("ACCESS_KEY:", ACCESS_KEY)
print("SECRET_KEY:", SECRET_KEY)
print("REGION:", REGION)


bucket = "retailio-data-lake-mzmi"

session = boto3.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=REGION
)

dataset = {
    "products":"data/products.csv",
    "customers":"data/customers.csv",
    "sales":"data/sales.csv"
}

for name, path in dataset.items():
    print(f"Processing {name}...")   
    if os.path.exists(path):
        df = pd.read_csv(path, encoding='latin1')
        print(f"Uploading {name} to S3...")
        wr.s3.to_parquet(
            df=df,
            path=f"s3://{bucket}/raw/{name}",
            index = False,
            mode = "overwrite",
            dataset = True,
            boto3_session=session,
        )
        print(f"Done uploading {name}.")
    else:
        print(f"File {path} does not exist.")
