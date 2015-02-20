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
parser.add_argument('-r', '--root', type=unicode)
parser.add_argument('index', type=unicode)
parser.add_argument('path_to_scan', type=unicode)
args = parser.parse_args()


verbose = args.verbose

logical_root = os.path.abspath(os.path.expanduser(args.root or args.path_to_scan))
root = os.path.abspath(os.path.expanduser(args.path_to_scan))

if not (root + '/').startswith(logical_root + '/'):
    print 'Path must be within root.'
    exit()


index = Index(args.index)
root_dev = os.stat(root).st_dev

con = index.connect()
cur = con.execute('INSERT INTO scans (started_at, hostname, root) VALUES (?, ?, ?)', [
    datetime.datetime.utcnow(),
    socket.gethostname(),
    logical_root
])
scan_id = cur.lastrowid

con.execute('BEGIN')
last_time = time()


files_total = 0
bytes_total = 0
files_read = 0
bytes_read = 0


for dir_path, dir_names, file_names in os.walk(root):


    # TODO: Remove dir_names from the list if they are on a different device.
    # TODO: Remove directories in the database that no longer exist.

    rel_dir_path = os.path.relpath(dir_path, logical_root)
    if verbose >= 2:
        print ' ', rel_dir_path + '/'

    # Get or create the directory.
    row = con.execute('SELECT id FROM directories WHERE path = ?', [rel_dir_path]).fetchone()
    if row:
        dir_id = row[0]
    else:
        cur = con.execute('INSERT OR IGNORE INTO directories (path) VALUES (?)', [rel_dir_path])
        dir_id = cur.lastrowid

    # Grab the existing files.
    existing_by_name = {}
    for row in con.execute('SELECT * FROM files WHERE dir_id = ?', [dir_id]):
        existing_by_name[row['name']] = row

    for file_name in file_names:

        path = os.path.join(dir_path, file_name)
        try:
            stat = os.lstat(path) # Does NOT follow symlinks.
        except OSError as e:
            if e.errno == errno.ENOENT:
                # The file vanished...
                # TODO: report this somehow.
                continue
            else:
                raise

        # We only like regular files.
        if not S_ISREG(stat.st_mode):
            continue

        # Moved to a different device.
        if stat.st_dev != root_dev:
            continue

        files_total += 1
        bytes_total += stat.st_size

        rel_path = os.path.relpath(path, logical_root)

        # Skip this file if it already exists, and does not appear to have
        # changed (warranting a new checksum).
        existing = existing_by_name.pop(file_name, None)
        if existing and (
            existing['size'] == stat.st_size and
            existing['mtime'] == stat.st_mtime and
            existing['ctime'] == stat.st_ctime
        ):
            if verbose >= 3:
                print ' ', rel_path
            continue

        files_read += 1
        bytes_read += stat.st_size

        if verbose >= 1:
            print '+', rel_path

        checksum = checksum_file(path)

        con.execute('''
            INSERT OR REPLACE INTO files
            (dir_id, scan_id, name, size, mtime, ctime, checksum)
            VALUES
            (?, ?, ?, ?, ?, ?, ?)
        ''', [
            dir_id, scan_id, file_name, stat.st_size, stat.st_mtime, stat.st_ctime, buffer(checksum)
        ])

        # Commit periodically.
        this_time = time()
        if this_time - last_time > 1:
            con.execute('COMMIT')
            con.execute('BEGIN')
            last_time = this_time

    for existing in existing_by_name.itervalues():
        rel_path = os.path.join(rel_dir_path, existing['name'])
        if verbose >= 1:
            print '-', rel_path
    if existing_by_name:
        con.execute(
            'DELETE FROM files WHERE dir_id = ? AND name IN (%s)' %
            ', '.join('?' * len(existing_by_name)), [dir_id] + existing_by_name.keys()
        )


con.execute('COMMIT')


print bytes_read, 'bytes read from', files_read, 'files'
print bytes_total, 'bytes total from', files_total, 'files'

