import sys
from aiocouch import CouchDB
from aiocouch.document import Document
import asyncio
import logging
import couchdb2
import json
from miniopy_async import Minio
import io
import time
from multiprocessing import Process, current_process, Pool, Lock, cpu_count
from math import floor

debug_file = "debug_1.log"
s3_id_file = "s3_id.txt"
diff_id_file = "diff_id.txt"
couch_id_file = "couchdb_ids.txt"
property_file = "couch-minio.json"

logging.basicConfig(level=logging.INFO,filename=debug_file,filemode='w')
# INFO:root:a - not attachment
# INFO:root:d - bad get doc
# INFO:root:p - bad get data attachment
# INFO:root:w - write minio
# INFO:root:r - read minio
# INFO:root:m - bad write minio

def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [ alist[i*length // wanted_parts: (i+1)*length // wanted_parts] 
            for i in range(wanted_parts) ]

def log(lock, message):
    lock.acquire()
    logging.info(f'{message}')
    lock.release()

async def readCouch(lock, proc_list_id, client, prop):
    async with CouchDB(prop.get("couch_path"), prop.get("couch_user"), prop.get("couch_password")) as couchdb:
        db = await couchdb[prop.get("couch_base")]
        for id in proc_list_id:
            try:
                doc_obj = await db.get(id)
            except Exception:
                message = f'd {id}'
                log(lock, message)
                continue
            if "_attachments" in doc_obj:
                last_id = ""
                for key in doc_obj['_attachments'].keys():
                    if last_id == id:
                        continue

                    last_id = id
                    file_name = id
                    if "." in key:
                        file_extension = key.split(".")[-1]
                        file_name = ".".join([id, file_extension])

                    attachment = doc_obj.attachment(key)
                    try:
                        data = await attachment.fetch()
                    except Exception:
                        message = f'p {id}'
                        log(lock, message)
                        continue
                    
                    try:
                        result = await client.put_object(prop.get("minio_base"), file_name, io.BytesIO(data), length=-1, part_size=10*1024*1024,)
                        message = f'w {id}'
                        log(lock, message)

                        message = f'r {id} {file_name}'
                        response = await client.get_object(prop.get("minio_base"), file_name)
                        log(lock, message)
                    except Exception:
                        message = f'm {id} {file_name}'
                        log(lock, message)
                        continue
                    
            else:
                message = f'a {id}'
                log(lock, message)
                continue


def read_write(lock, proc_list, client, prop):
    asyncio.run(readCouch(lock, proc_list, client, prop))  

def to_s3_files():
    dst_file = open(s3_id_file, 'a')
    with open(debug_file, 'r') as src_file:
        for line in src_file:
            if line.startswith("INFO:root:w"):
                id = line.split(" ")[1]
                dst_file.write(id)

    dst_file.close()      


def main():
    lock = Lock()
    logging.info(f"start")
    start = time.time()
    
    with open(property_file) as f:
        prop = json.load(f)

    try:
        couch = couchdb2.Server(href=prop.get("couch_path"),
                                username=prop.get("couch_user"),
                                password=prop.get("couch_password"),
                                use_session=True,
                                ca_file=None)

        client = Minio(
            prop.get("minio_path"),
            access_key=prop.get("minio_user"),
            secret_key=prop.get("minio_password"),
            secure=prop.get("minio_ssl"),
            region=prop.get("minio_region"),
    )
    except Exception:
        logging.info("CouchDB or Minio not connection")
        exit()
        
    db = couch.get(prop.get("couch_base"))
    minio_base = prop.get("minio_base")

    NUM_CORES = cpu_count()

    all_list_id = []

    #with open(couch_id_file, 'r') as f:
    #    all_list_id = f.read().splitlines()
    with open(couch_id_file, 'w') as f:
        for couch_id in db.ids():
            all_list_id.append(couch_id)    
            f.write(f'{couch_id}\n')

    s3_file = open(s3_id_file, 'a+')
    s3_file.close
    s3_id = []
    with open(s3_id_file, 'r') as f:
        s3_id = f.read().splitlines()

    dst = open(diff_id_file, 'w')
    diff_list_id = list(set(all_list_id)-set(s3_id))
    for id in diff_list_id:
        dst.write(id+'\n')
    dst.close()

    proc_lists = split_list(diff_list_id, NUM_CORES)
    procs = []
    for proc_list in proc_lists:
        proc = Process(target=read_write, args=(lock, proc_list, client, prop))
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()

    logging.info("finish")
    end = time.time()
    logging.info(f"duration: {end - start} seconds")

if __name__ == "__main__":
    main()
    to_s3_files()
