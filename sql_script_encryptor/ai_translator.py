import openai
from SQLScriptEncryptor import *
import os

# instruction prompt
fixed_prompt = """
Translate the given SQL code into equivalent T-SQL. 
Assume the input SQL code is written in a generic SQL dialect, 
and you need to convert it into T-SQL compatible with Microsoft SQL Server:\n
"""


# Function to interact with GPT-3 API
def generate_response(sql_script_input: str, api_key: str):
    prompt = fixed_prompt + sql_script_input
    response = openai.Completion.create(
        engine="text-davinci-003",
        prompt=prompt,
        max_tokens=500,
        api_key=api_key
    )
    return response.choices[0].text.strip()


if __name__ == "__main__":

    data_path = "PREPROCESSING_ESTIMATION/"
    file_name = "2_COMMON_OPERATIONS_FIRST_PART.sql"

    file = read_sql_file(data_path + file_name)

    encrypted_sql_script, name_mappings = encrypt_sql_script(
        file,
        manual_encryption_ls=["DATE_STOP_ESTIMA", "DATE_START_ESTIMA", "PARAM_PREPROCESSING_0_0"]
    )

    save_name_mappings_to_file(name_mappings, file_name)

    api_key = os.getenv("API_KEY")
    # Generate response using GPT-3 API
    encrypted_response = generate_response(encrypted_sql_script, api_key=api_key)

    # Print GPT-3 response
    print("GPT-3 Response (encrypted):")
    print(encrypted_response)

    loaded_name_mappings = load_name_mappings_from_file(file_name)
    decrypted_response = decrypt_sql_script(encrypted_response, loaded_name_mappings)

    print("\n[Decrypted] SQL Script:")
    print(decrypted_response)
