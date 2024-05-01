import re
import hashlib
import json
import os


# Function to generate encrypted names
def generate_encrypted_name(name: str):
    encrypted_name = hashlib.sha256(name.encode()).hexdigest()[:10]
    return encrypted_name


# Function to encrypt words within double quotation marks   
def encrypt_sql_script(text: str, manual_encryption_ls: list[str]):

    to_encrypt = []

    if manual_encryption_ls:
        for word in manual_encryption_ls:
            if word in text:
                to_encrypt.append(word)
                print(f"word: {word} added to encryption.")

    name_mappings = {}  # Dictionary to store original words and corresponding encrypted words

    def encrypt_match(match):
        word = match.group(0)
        encrypted_words = []
        #print("Processing word:", word)
        if word.startswith('"') and word.endswith('"'):
            # Remove double quotes and encrypt the word
            word_without_quotes = word[1:-1]
            encrypted_word = generate_encrypted_name(word_without_quotes)
            name_mappings[word_without_quotes] = encrypted_word
            encrypted_words.append(f'"{encrypted_word}"')

        # if word in manual_encryption_ls:
        #     encrypted_word = generate_encrypted_name(word)
        #     name_mappings[word] = encrypted_word
        #     encrypted_words.append(encrypted_word)
        #
        if '_' in word and not (word.startswith('"') and word.endswith('"')):
            word_without_quotes = word.strip('"')
            encrypted_word = generate_encrypted_name(word_without_quotes)
            name_mappings[word_without_quotes] = encrypted_word
            encrypted_words.append(encrypted_word)

        return encrypted_words[0] if encrypted_words else word

    # pattern = r'"[^"]*"'
    pattern = r'"[^"]*"|\b(?:' + '|'.join(re.escape(word) for word in to_encrypt) + r')\b'
    encrypted_text = re.sub(pattern, encrypt_match, text)
    return encrypted_text, name_mappings


# Function to save the name mapping to a local file with a specific name
def save_name_mappings_to_file(name_mappings: dict, input_filename: str):
    base_filename, _ = os.path.splitext(input_filename)
    output_filename = f"{base_filename}_mapping_keys.json"
    
    with open(output_filename, 'w') as sql_file:
        json.dump(name_mappings, sql_file)


# Function to load name mapping from a local file
def load_name_mappings_from_file(input_filename: str) -> dict[str, str]:
    base_filename, _ = os.path.splitext(input_filename)
    mapping_filename = f"{base_filename}_mapping_keys.json"
    
    if os.path.exists(mapping_filename):
        with open(mapping_filename, 'r') as sql_file:
            name_mapping = json.load(sql_file)
        return name_mapping
    else:
        print(f"No mapping file found for '{input_filename}'.")
        return {}


def read_sql_file(sql_file_path: str):
    with open(sql_file_path, 'r') as sql_file:
        sql_file = sql_file.read()
        return sql_file


# Function to decrypt encrypted SQL script
def decrypt_sql_script(encrypted_sql_script: str, name_mappings: dict) -> str:
    # Reverse the mapping to get original names from encrypted names
    reverse_mapping = {v: k for k, v in name_mappings.items()}

    # Use the reverse mapping to replace encrypted names with original names
    decrypted_script = re.sub(
        r'\b[a-fA-F0-9]{10}\b',
        lambda x: reverse_mapping.get(x.group(), x.group()),
        encrypted_sql_script
    )
    
    return decrypted_script

def create_or_clean(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    else:
        # clear the folder
        for file in os.listdir(folder_name):
            os.remove(os.path.join(folder_name, file))

# Example usage
if __name__ == "__main__":

    encrypt = False # if false will decrypt
    test = True # if true will test the encryption and decryption

    folder_name = "2_COMMON_OPERATIONS_FIRST_PART"
    file_name = "2_COMMON_OPERATIONS_FIRST_PART"
    manual_encryption_ls = ["DATE_STOP_ESTIMA", "DATE_START_ESTIMA", "PARAM_PREPROCESSING_0_0"]

    if encrypt or test:
        create_or_clean(folder_name)

        with open("source_code_to_encrypt", "r", encoding='utf-8') as f:
            sql_script = f.read()


        encrypted_sql_script, name_mappings = encrypt_sql_script(
            sql_script,
            manual_encryption_ls=manual_encryption_ls
        )

        with open(os.path.join(folder_name, file_name + '.json'), 'w', encoding='utf-8') as f:
            json.dump(name_mappings, f, indent=4)

        with open(os.path.join(folder_name, 'encrypted' + file_name + '.sql'), 'w', encoding = 'utf-8') as f:
            f.write(encrypted_sql_script)

        # Print encrypted SQL script
        print("\nEncrypted SQL Script:")
        print(encrypted_sql_script)

    if (not encrypt) or test:

        if not test:
            with open("source_code_to_decrypt", "r", encoding='utf-8') as f:
                encrypted_sql_script = f.read()

        with open(os.path.join(folder_name, file_name + '.json'), 'r', encoding='utf-8') as f:
            name_mappings = json.load(f)

        decrypted_sql_script = decrypt_sql_script(encrypted_sql_script, name_mappings)

        # Print decrypted SQL script
        print("\nDecrypted SQL Script:")
        print(decrypted_sql_script)

        if test:
            assert decrypted_sql_script == sql_script