import re
from os import getenv
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI
from transformers import AutoTokenizer, PreTrainedTokenizerFast

from src.utils.tokenizer import count_tokens


def convert_text_to_markdown(
    model_openrouter: str,
    raw_text: str,
    template: str,
    apply_simple_formatting: bool = True,
    apply_llm_formatting: bool = True,
) -> str:
    """
    Converts raw text into clean, properly formatted markdown. The method allows
    optional application of simple formatting (HTML entity replacement and nested
    list fixing) and LLM-based formatting.

    Args:
        model_openrouter (str): The model to use for transformation.
        raw_text (str): The input text to be formatted.
        template (str): The template to use for the prompt.
        apply_simple_formatting (bool): Whether to apply simple formatting.
        apply_llm_formatting (bool): Whether to apply LLM-based formatting.

    Returns:
        str: The formatted markdown text.
    """
    # Step 1: Apply simple formatting if enabled
    if apply_simple_formatting:
        formatted_text_simple = _simple_markdown_formatting(raw_text)
    else:
        formatted_text_simple = raw_text

    # Step 2: Apply LLM formatting if enabled
    if apply_llm_formatting:
        formatted_text_llm = _apply_llm_formatting(
            model_openrouter, formatted_text_simple, template
        )
    else:
        formatted_text_llm = formatted_text_simple

    return formatted_text_llm


def convert_long_text_to_markdown(
    model_openrouter: str,
    raw_text: str,
    template: str,
    tokenizer: PreTrainedTokenizerFast,
    max_tokens: int,
    apply_simple_formatting: bool = True,
    apply_llm_formatting: bool = True,
) -> str:
    """
    Processes the input text into Markdown by splitting it into sections,
    estimating the token count for each section, combining sections into
    batches that almost reach (but do not surpass) the specified token limit,
    and then formatting each batch.

    Args:
        model_openrouter (str): The model to use for Markdown transformation.
        raw_text (str): The input text to process.
        template (str): The template to use for formatting.
        tokenizer (PreTrainedTokenizerFast): The tokenizer to use for counting tokens.
        max_tokens (int): The maximum number of tokens per batch.
        apply_simple_formatting (bool): Whether to apply simple formatting.
        apply_llm_formatting (bool): Whether to apply LLM-based formatting.

    Returns:
        str: The processed Markdown text.
    """
    # Step 1: Divide the text into sections
    sections = _divide_into_sections(raw_text)

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
        processed_text = convert_text_to_markdown(
            model_openrouter,
            combined_text,
            template,
            apply_simple_formatting=apply_simple_formatting,
            apply_llm_formatting=apply_llm_formatting,
        )
        processed_sections.append(processed_text)

    # Step 5: Concatenate the processed sections back into a single text
    processed_text = "\n\n".join(processed_sections)

    return processed_text


def _simple_markdown_formatting(text: str) -> str:
    """
    A private method to clean and format raw text for markdown conversion.
    It replaces specific HTML entities and fixes nested list formatting.

    Args:
        text (str): The raw input text.

    Returns:
        str: The cleaned and formatted text.
    """
    # Step 1: Manually replace specific HTML entities
    formatted_text_simple = (
        text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    )

    # Step 2: Fix nested list formatting for any number of levels
    formatted_text_simple = re.sub(
        r"^(\*+)(.*)",
        lambda match: "  " * (len(match.group(1)) - 1) + "*" + match.group(2),
        formatted_text_simple,
        flags=re.MULTILINE,
    )

    return formatted_text_simple


def _apply_llm_formatting(model_openrouter: str, text: str, template: str) -> str:
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
    Divides the input text into sections based on titles marked with `=` signs.

    Args:
        text (str): The input text to be divided into sections.

    Returns:
        list: A list of sections, where each section includes the title and its
              related content.
    """
    # Regex to identify sections based on titles marked with `=` signs
    section_regex = r"(={1,5}\s*[^=]+\s*={1,5})"

    # Split the text into sections using the regex
    sections = re.split(section_regex, text)

    # Combine titles with their directly related content
    combined_sections = []
    for i in range(1, len(sections), 2):  # Start from 1 to skip the first empty element
        title = sections[i]
        content = sections[i + 1].strip()  # Remove leading/trailing whitespace
        combined_sections.append(f"{title}\n{content}")

    return combined_sections


if __name__ == "__main__":

    # Load Environment variables (OpenRouter API Key)
    load_dotenv()

    # Base Path
    base_path = Path("../")

    # Load data
    print("Loading data...")
    articles = pd.read_parquet(base_path / "data/processed/articles.parquet")
    short_article = articles[articles["id"] == 8].iloc[0]
    long_article = articles[articles["id"] == 3077].iloc[0]

    # Load HF tokenizer
    huggingface_token = getenv("HUGGINGFACE_TOKEN")
    model_hf = "deepseek-ai/DeepSeek-V3"
    tokenizer = AutoTokenizer.from_pretrained(model_hf, token=huggingface_token)

    print(f"\nShort text title: {short_article['title']}")
    print(f"Number of tokens: {count_tokens(tokenizer, short_article['text'])}")
    print(f"\nLong text title: {long_article['title']}")
    print(f"Number of tokens: {count_tokens(tokenizer, long_article['text'])}")

    # Load prompt
    prompts_path = base_path / "prompts.yaml"
    with open(prompts_path, "r") as file:
        prompts = yaml.safe_load(file)

    # Format text
    model_openrouter = "deepseek/deepseek-chat"

    print("\nFormatting short article...")
    short_article_formatted = convert_text_to_markdown(
        model_openrouter=model_openrouter,
        raw_text=short_article["text"],
        template=prompts["format_markdown"],
        apply_simple_formatting=True,
        apply_llm_formatting=True,
    )

    print(short_article_formatted)

    print("\n==============================")
    print("\nFormatting long article...")
    long_article_formatted = convert_long_text_to_markdown(
        model_openrouter=model_openrouter,
        raw_text=long_article["text"],
        template=prompts["format_markdown"],
        tokenizer=tokenizer,
        max_tokens=7000,  # Max 8k, but we have a prompt
        apply_simple_formatting=True,
        apply_llm_formatting=True,
    )

    print(long_article_formatted)
