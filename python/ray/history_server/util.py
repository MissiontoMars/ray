

def encrypt_aes(key, raw, now=None):
    try:
        from Crypto.Cipher import AES
        from Crypto import Random
        from binascii import hexlify
    except Exception:
        return None

    length = 16
    count = len(raw)
    add = length - (count % length)
    raw = raw + ("\0" * add)
    iv = Random.new().read(AES.block_size)
    aes_key = (key[:8] + now) if now else key
    cipher = AES.new(aes_key.encode(), AES.MODE_CBC, iv)
    result = iv + cipher.encrypt(raw.encode())
    return hexlify(result)
