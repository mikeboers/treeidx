import argparse
import datetime
import errno
import os
import socket
from time import time
from stat import S_ISREG

from treeidx.index import Index
from treeidx.checksum import checksum_file



parser = argparse.ArgumentParser()
parser.add_argument('-v', '--verbose', action='count', default=0)
parser.add_argument('index')
parser.add_argument('root')
args = parser.parse_args()


verbose = args.verbose
root = os.path.abspath(args.root)


index = Index(args.index)
root_dev = os.stat(root).st_dev

con = index.connect()
cur = con.execute('INSERT INTO scans (started_at, hostname, root) VALUES (?, ?, ?)', [
    datetime.datetime.utcnow(),
    socket.gethostname(),
    root
])
scan_id = cur.lastrowid

con.execute('BEGIN')
last_time = time()


for dir_path, dir_names, file_names in os.walk(root):


    # TODO: Remove dir_names from the list if they are on a different device.

    if verbose:
        print ' ', dir_path[len(root) + 1:] + '/'

    for file_name in file_names:

        path = os.path.join(dir_path, file_name)
        try:
            stat = os.lstat(path) # Does NOT follow symlinks.
        except OSError as e:
            if e.errno == errno.ENOENT:
                continue
            else:
                raise

        # We only like regular files.
        if not S_ISREG(stat.st_mode):
            continue

        # Moved to a different device.
        if stat.st_dev != root_dev:
            continue

        rel_path = path[len(root) + 1:]

        # Look for an existing file.
        row = con.execute('SELECT id, size, mtime, ctime FROM files WHERE path = ? LIMIT 1', [rel_path]).fetchone()
        if row:
            if row[1] == stat.st_size and row[2] == stat.st_mtime and row[3] == stat.st_ctime:
                if verbose >= 2:
                    print ' ', rel_path
                continue
            else:
                con.execute('DELETE FROM files WHERE id = ?', [row[0]])

        if verbose:
            print '+', rel_path

        checksum = checksum_file(path)

        con.execute('INSERT INTO files (scan_id, path, size, mtime, ctime, checksum) VALUES (?, ?, ?, ?, ?, ?)',
                    [scan_id, rel_path, stat.st_size, stat.st_mtime, stat.st_ctime, buffer(checksum)]
        )

        # Commit periodically.
        this_time = time()
        if time_time - last_time > 1:
            con.execute('COMMIT')
            con.execute('BEGIN')
            last_time = this_time


con.execute('COMMIT')

