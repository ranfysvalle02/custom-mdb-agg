import logging
import ollama

def prompt_operator(doc, args):
    """
    Custom operator to interact with an LLM using a prompt.

    Parameters:
    - doc (dict): The current document.
    - args (list): Arguments for the operator.

    Returns:
    - The result from the LLM.
    """
    desired_model = 'llama3.2:3b'
    logger = logging.getLogger(__name__)

    if len(args) != 2:
        raise ValueError("$prompt requires two arguments: field name and prompt text")

    field_name = args[0]
    prompt_text = args[1]
    # Get the value from the document
    field_value = doc.get(field_name)
    if field_value is None:
        return None

    # Construct the message
    message_content = f"""
[prompt]
{prompt_text}
[/prompt]
[context]
field: {field_name}
value:
{str(field_value)}
[full document]
{str(doc)}
[/full document]
[/context]
"""

    try:
        response = ollama.chat(model=desired_model, messages=[
            {
                'role': 'user',
                'content': message_content.strip(),
            },
        ])
        # Extract the response content
        result = response['message']['content']
    except Exception as e:
        logger.error(f"Error in $prompt operator: {e}")
        result = None

    return result
