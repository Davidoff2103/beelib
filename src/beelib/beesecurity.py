import base64
import hashlib
import os
from Crypto import Random
from Crypto.Cipher import AES

def pad(s):
    """
    Añade espacios al final del texto para que su longitud sea múltiplo del tamaño de bloque de AES.

    Args:
        s (str): Texto plano.

    Returns:
        str: Texto con padding de espacios.
    """
    block_size = AES.block_size
    remainder = len(s) % block_size
    padding_needed = block_size - remainder
    return s + padding_needed * ' '

def un_pad(s):
    """
    Elimina los espacios añadidos al final del texto durante el padding.

    Args:
        s (bytes): Texto descifrado con padding.

    Returns:
        bytes: Texto sin padding.
    """
    return s.rstrip()

def encrypt(plain_text, password):
    """
    Cifra un texto plano usando AES-256-CBC con clave derivada mediante scrypt.

    Args:
        plain_text (str): Texto a cifrar.
        password (str): Contraseña base para derivar la clave.

    Returns:
        str: Cadena cifrada codificada en base64 que incluye el ciphertext + salt + IV.
    """
    # Genera una sal aleatoria
    salt = os.urandom(AES.block_size)

    # Genera un vector de inicialización (IV) aleatorio
    iv = Random.new().read(AES.block_size)

    # Deriva la clave privada usando scrypt
    private_key = hashlib.scrypt(
        password.encode(), salt=salt, n=2**14, r=8, p=1, dklen=32
    )

    # Aplica padding al texto plano
    padded_text = pad(plain_text)

    # Crea el cifrador AES en modo CBC
    cipher_config = AES.new(private_key, AES.MODE_CBC, iv)

    # Cifra el texto y concatena ciphertext + salt + IV codificados en base64
    encrypted = base64.b64encode(cipher_config.encrypt(padded_text.encode()))
    return (encrypted + base64.b64encode(salt) + base64.b64encode(iv)).decode()

def decrypt(enc_str, password):
    """
    Descifra una cadena cifrada con AES-256-CBC y clave derivada con scrypt.

    Args:
        enc_str (str): Cadena cifrada y codificada en base64 (ciphertext + salt + IV).
        password (str): Contraseña para derivar la clave de descifrado.

    Returns:
        str: Texto descifrado.
    """
    # Extrae y decodifica IV y salt desde el final de la cadena
    iv = base64.b64decode(enc_str[-24:])
    salt = base64.b64decode(enc_str[-48:-24])
    enc = base64.b64decode(enc_str[:-48])

    # Deriva la misma clave con scrypt usando la misma sal
    private_key = hashlib.scrypt(
        password.encode(), salt=salt, n=2**14, r=8, p=1, dklen=32
    )

    # Crea el cifrador AES con la clave y el IV
    cipher = AES.new(private_key, AES.MODE_CBC, iv)

    # Descifra el contenido
    decrypted = cipher.decrypt(enc)

    # Elimina el padding
    original = un_pad(decrypted)

    return original.decode('utf-8')
