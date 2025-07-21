import base64
import hashlib
import os

from Crypto import Random
from Crypto.Cipher import AES


def pad(s):
    """
    Pad the text with spaces to make it a multiple of 16 bytes for AES encryption.

    Args:
        s (str): The input string to be padded.

    Returns:
        str: Padded string.
    """
    block_size = AES.block_size
    remainder = len(s) % block_size
    padding_needed = block_size - remainder
    return s + padding_needed * ' '

def un_pad(s):
    """
    Remove the padding from the text after decryption.

    Args:
        s (str): The input string with padding.

    Returns:
        str: Unpadded string.
    """
    return s.rstrip()

def encrypt(plain_text, password):
    """
    Encrypt the plain text using AES with a given password.

    Args:
        plain_text (str): The text to be encrypted.
        password (str): The encryption password.

    Returns:
        str: Base64 encoded encrypted string.
    """
    # Generate a random salt
    salt = os.urandom(AES.block_size)

    # Generate a random initialization vector (IV)
    iv = Random.new().read(AES.block_size)

    # Use Scrypt to derive a private key from the password and salt
    private_key = hashlib.scrypt(password.encode(), salt=salt, n=2 ** 14, r=8, p=1, dklen=32)

    # Pad the text with spaces for AES CBC mode
    padded_text = pad(plain_text)

    # Create the cipher configuration
    cipher_config = AES.new(private_key, AES.MODE_CBC, iv)

    # Return the base64 encoded encrypted string along with salt and IV
    return (base64.b64encode(cipher_config.encrypt(padded_text.encode())) + 
            base64.b64encode(salt) + 
            base64.b64encode(iv)).decode()

def decrypt(enc_str, password):
    """
    Decrypt the encrypted string using AES with a given password.

    Args:
        enc_str (str): The base64 encoded encrypted string.
        password (str): The decryption password.

    Returns:
        str: Decrypted plain text.
    """
    # Decode the dictionary entries from base64
    iv = base64.b64decode(enc_str[-24:])
    salt = base64.b64decode(enc_str[-48:-24])
    enc = base64.b64decode(enc_str[:-48])

    # Generate the private key from the password and salt
    private_key = hashlib.scrypt(password.encode(), salt=salt, n=2 ** 14, r=8, p=1, dklen=32)

    # Create the cipher configuration
    cipher = AES.new(private_key, AES.MODE_CBC, iv)

    # Decrypt the cipher text
    decrypted = cipher.decrypt(enc)

    # Unpad the text to remove the added spaces
    original = un_pad(decrypted)

    return original.decode('utf-8')
