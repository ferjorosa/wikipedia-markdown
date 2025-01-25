from os import getenv

from dotenv import load_dotenv
from openai import OpenAI

if __name__ == "__main__":

    # Load Local environment variables (OpenAI API Key)
    load_dotenv()
    openai_token = getenv("OPENAI_TOKEN")

    # Initialize the OpenAI client
    client = OpenAI(api_key=openai_token)

    batch_id = "batch_6795146d7d00819082a8ba4463cabf73"

    # Check Batch job
    batch = client.batches.retrieve(batch_id)
    print(batch)

    # Cancel batch job
    # result = client.batches.cancel(batch_id)
    # print(result)
