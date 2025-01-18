from transformers import PreTrainedTokenizerFast


def count_tokens(tokenizer: PreTrainedTokenizerFast, text: str) -> int:
    """
    Count the number of tokens in a given text using the provided tokenizer.

    Args:
        tokenizer (PreTrainedTokenizerFast): The tokenizer to use for encoding the text.
        text (str): The input text to tokenize.

    Returns:
        int: The number of tokens in the tokenized text.
    """
    return len(tokenizer.encode(text))
