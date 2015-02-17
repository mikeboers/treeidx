from hashlib import sha256


def checksum_file(path, chunk_size=2**14, progress=None):
    
    hasher = sha256()
    bytes_read = 0
    with open(path, 'rb') as fh:
        while True:
            chunk = fh.read(chunk_size)
            if progress:
                bytes_read += len(chunk)
                progress(bytes_read)
            if chunk:
                hasher.update(chunk)
            else:
                break

    return hasher.digest()

