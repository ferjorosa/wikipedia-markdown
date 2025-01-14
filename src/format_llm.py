import re
from os import getenv

from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI
from transformers import PreTrainedTokenizerFast

from src.utils.tokenizer import count_tokens


def clean_text(
    model_openrouter: str,
    raw_text: str,
    template: str,
) -> str:
    """
    Converts raw text into clean, properly formatted markdown using a specified
    model and template. The function leverages LLM-based formatting to transform
    the input text into structured markdown.

    Args:
        model_openrouter (str): The model to use for the transformation, as
        provided by OpenRouter.
        raw_text (str): The input text to be formatted into markdown.
        template (str): The template to use for constructing the prompt sent to
        the model.

    Returns:
        str: The formatted markdown text after applying LLM-based formatting.
    """
    return _apply_llm_formatting(model_openrouter, raw_text, template)


def clean_long_text(
    model_openrouter: str,
    raw_text: str,
    template: str,
    tokenizer: PreTrainedTokenizerFast,
    max_tokens: int,
):
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
        cleaned_text = clean_text(model_openrouter, combined_text, template)
        processed_sections.append(cleaned_text)

    # Step 5: Concatenate the processed sections back into a single text
    processed_text = "\n\n".join(processed_sections)

    return processed_text


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
    section_regex = r"(={1,6}\s*[^=]+\s*={1,6})"

    # Split the text into sections using the regex
    sections = re.split(section_regex, text)

    # Combine titles with their directly related content
    combined_sections = []
    for i in range(1, len(sections), 2):  # Start from 1 to skip the first empty element
        title = sections[i]
        content = sections[i + 1].strip()  # Remove leading/trailing whitespace
        combined_sections.append(f"{title}\n{content}")

    return combined_sections
