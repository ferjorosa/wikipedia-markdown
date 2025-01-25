import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from os import getenv
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI
from tqdm import tqdm
from transformers import AutoTokenizer, PreTrainedTokenizerFast

from wikipedia_markdown.utils.database import (
    get_ids_with_empty_llm_cleaned_text,
    get_rows_from_ids,
    update_llm_cleaned_row,
)
from wikipedia_markdown.utils.tokenizer import count_tokens
from wikipedia_markdown.utils.yaml import load_yaml


def clean_articles(
    db_path: Path,
    model_openrouter: str,
    tokenizer: PreTrainedTokenizerFast,
    prompt: str,
    max_workers: int,
    max_tokens: int,
    max_rows: Optional[int] = None,
    debug: bool = False,
):
    """
    Cleans the `markdown_text` for rows in the database where `llm_cleaned_text`
    is empty or NULL. Uses `clean_text` for short texts and `clean_long_text` for
    long texts.

    Args:
        db_path (Path): Path to the SQLite database file.
        model_openrouter (str): The model to use for cleaning, as provided by
        OpenRouter.
        tokenizer (PreTrainedTokenizerFast): Tokenizer to count tokens in the
        text.
        max_workers (int): Maximum number of workers for parallel processing.
        max_tokens (int): Token limit to decide between `clean_text` and
        `clean_long_text`.
        max_rows (int): Maximum number of rows to process.
        debug (bool): If True, print debug messages for database updates
        (default: False).
    """
    # Get the list of IDs where `llm_cleaned_text` is empty or NULL
    ids = get_ids_with_empty_llm_cleaned_text(db_path, limit=max_rows)

    if not ids:
        print("No rows to clean.")
        return

    # Fetch rows corresponding to the IDs
    rows = get_rows_from_ids(db_path, ids)

    # Function to process a single row
    def process_row(row):
        try:
            markdown_text = row["markdown_text"]
            if count_tokens(tokenizer, markdown_text) > max_tokens:
                # Use clean_long_text for long texts
                cleaned_text = clean_long_text(
                    model_openrouter=model_openrouter,
                    text=markdown_text,
                    template=prompt,
                    tokenizer=tokenizer,
                    max_tokens=max_tokens,
                )
            else:
                # Use clean_text for short texts
                cleaned_text = clean_text(
                    model_openrouter=model_openrouter,
                    text=markdown_text,
                    template=prompt,
                )

            # Calculate the token count for the cleaned text
            cleaned_text_tokens = count_tokens(tokenizer, cleaned_text)

            # Return the row ID, cleaned text, and token count
            return row["id"], cleaned_text, cleaned_text_tokens

        except Exception as e:
            print(f"Error processing row {row['id']}: {e}")
            return None

    # Process rows in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_row, row) for row in rows]

        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Cleaning rows"
        ):
            result = future.result()
            if result:
                row_id, cleaned_text, cleaned_text_tokens = result

                # Update the database using the existing method
                update_llm_cleaned_row(
                    db_path=db_path,
                    id=row_id,
                    model=model_openrouter,
                    llm_cleaned_text=cleaned_text,
                    llm_cleaned_text_tokens=cleaned_text_tokens,
                    debug=debug,
                )


def clean_text(
    model_openrouter: str,
    text: str,
    template: str,
) -> str:
    """
    Converts raw text into clean, properly formatted markdown using a specified
    model and template. The function leverages LLM-based formatting to transform
    the input text into structured markdown.

    Args:
        model_openrouter (str): The model to use for the transformation, as
        provided by OpenRouter.
        text (str): The input text to be cleaned.
        template (str): The template to use for constructing the prompt sent to
        the model.

    Returns:
        str: The formatted markdown text after applying LLM-based formatting.
    """
    return _apply_llm_formatting(model_openrouter, text, template)


def clean_long_text(
    model_openrouter: str,
    text: str,
    template: str,
    tokenizer: PreTrainedTokenizerFast,
    max_tokens: int,
):
    # Step 1: Divide the text into sections
    sections = _divide_into_sections(text)

    # Step 2: Estimate the number of tokens for each section
    section_token_counts = [count_tokens(tokenizer, section) for section in sections]

    # Step 3: Combine sections into batches that almost reach the max_tokens limit
    batches = []
    current_batch = []
    current_token_count = 0

    for section, token_count in zip(sections, section_token_counts):
        if current_token_count + token_count <= max_tokens:
            current_batch.append(section)
            current_token_count += token_count
        else:
            batches.append(current_batch)
            current_batch = [section]
            current_token_count = token_count

    # Add the last batch if it's not empty
    if current_batch:
        batches.append(current_batch)

    # Step 4: Process each batch using the convert_text_to_markdown function
    processed_sections = []
    for batch in batches:
        combined_text = "\n\n".join(batch)
        cleaned_text = clean_text(model_openrouter, combined_text, template)
        processed_sections.append(cleaned_text)

    # Step 5: Concatenate the processed sections back into a single text
    processed_text = "\n\n".join(processed_sections)

    return processed_text


def _apply_llm_formatting(
    model_openrouter: str, text: str, template: str, request_timeout: int = 300
) -> str:
    """
    A private method to apply LLM-based formatting to the text.

    Args:
        model_openrouter (str): The model to use for transformation.
        text (str): The input text to be formatted.
        template (str): The template to use for the prompt.

    Returns:
        str: The formatted markdown text.
    """
    # Create a prompt template
    prompt = PromptTemplate(template=template, input_variables=["text"])

    # Initialize the language model
    llm = ChatOpenAI(
        openai_api_key=getenv("OPENROUTER_API_KEY"),
        openai_api_base="https://openrouter.ai/api/v1",
        model_name=model_openrouter,
        request_timeout=request_timeout,
    )

    # Initialize the output parser
    output_parser = StrOutputParser()

    # Create a chain for processing the text
    chain = prompt | llm | output_parser

    # Invoke the chain to format the text
    formatted_text_llm = chain.invoke(input={"text": text})
    return formatted_text_llm


def _divide_into_sections(text: str) -> list:
    """
    Divides the input text into sections based on Markdown headings marked with
    `#` signs.

    Args:
        text (str): The input text to be divided into sections.

    Returns:
        list: A list of sections, where each section includes the heading and its
              related content.
    """
    # Regex to identify sections based on Markdown headings marked with `#` signs
    # This regex allows for optional leading whitespace before the `#` characters
    section_regex = r"(^\s*#+\s?.*$)"

    # Split the text into sections using the regex
    sections = re.split(section_regex, text, flags=re.MULTILINE)

    # Combine headings with their directly related content
    combined_sections = []
    for i in range(1, len(sections), 2):  # Start from 1 to skip the first empty element
        heading = sections[
            i
        ].strip()  # Remove leading/trailing whitespace from the heading
        content = sections[
            i + 1
        ].strip()  # Remove leading/trailing whitespace from the content
        combined_sections.append(f"{heading}\n{content}")

    return combined_sections


if __name__ == "__main__":

    # Example text with Markdown headings
    text = """
        # Heading 1
        This is the content for heading 1.

        ## Heading 2
        This is the content for heading 2.

        ###Heading 3 (missing space)

        ###### Heading 4
        This is the content for heading 4.

        ####### Heading 5 (too many #)
        This is invalid because Markdown only allows up to 6 # characters.

        ##     Heading 6 (extra space)
        This is valid because extra spaces after ## are allowed.

        #
        This is a heading with no text.

        ##
        ##
    """

    # Divide the text into sections
    sections = _divide_into_sections(text)

    # Print each section
    for section in sections:
        print(section)
        print("---")

    # Example: cleaning long article text

    print("\n#####################\n")

    # Load Local environment variables (OpenRouter API Key)
    load_dotenv()

    huggingface_token = getenv("HUGGINGFACE_TOKEN")

    # Set the base path
    base_path = Path("../../")  # One level up from the current working directory

    # Load the YAML configuration
    config_path = base_path / "config.yaml"
    config = load_yaml(config_path)

    # Construct the full path to the database file
    data_folder = base_path / config["data_folder"]
    db_file = config["db_file"]
    db_path = data_folder / db_file

    # Print the database path for verification
    print(f"Database path: {db_path}")

    # Check if the database file exists
    if db_path.exists():
        print("Database file exists.")
    else:
        print("Database file does not exist.")

    model_openrouter = "deepseek/deepseek-chat"
    model_hf = "deepseek-ai/DeepSeek-V3"

    # model_openrouter = "openai/gpt-4o-mini"
    # model_hf = "Xenova/gpt-4o"

    tokenizer = AutoTokenizer.from_pretrained(model_hf, token=huggingface_token)

    prompts = load_yaml(base_path / "prompts.yaml")

    article_id = 61541  # Bobby Robson
    # article_id = 17 # Adobe Illustrator
    # article_id = 1 # April

    rows = get_rows_from_ids(
        db_path=db_path,
        ids=[article_id],
    )

    article = rows[0]

    # if article["markdown_text_tokens"] > 5000:
    #     # print("Long article\n")
    #     article_formatted = clean_long_text(
    #         model_openrouter=model_openrouter,
    #         text=article["markdown_text"],
    #         template=prompts["clean_markdown"],
    #         tokenizer=tokenizer,
    #         max_tokens=7000,  # Max 8k, but we have a prompt
    #     )
    # else:
    #     # print("Short article\n")
    #     article_formatted = clean_text(
    #         model_openrouter=model_openrouter,
    #         text=article["markdown_text"],
    #         template=prompts["clean_markdown"],
    #     )
    #
    # print("\n#####################\n")
    #
    # print(article_formatted)

    # Example: Cleaning one article from the DB using the clean method
    print("\n#####################\n")
    print("Cleaning one article from the DB using the clean method...")

    # Disable tokenizer parallelism (going to be called withing ThreadPool)
    os.environ["TOKENIZERS_PARALLELISM"] = "false"

    # Clean one article using the clean method
    clean_articles(
        db_path=db_path,
        model_openrouter=model_openrouter,
        tokenizer=tokenizer,
        prompt=prompts["clean_markdown"],
        max_workers=32,  # Number of parallel workers
        max_rows=2,  # Only clean one article
        max_tokens=5000,  # Token limit for clean_text vs clean_long_text
        debug=True,  # Print debug messages
    )

    print("Cleaning completed.")
