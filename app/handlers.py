import asyncio
import os
import time
from typing import Optional

from app.main import toggle_find_value, StreamEntry, StreamEntries, Item
from app.parsers import parse_redis_file_format

set_cmd = False
ack_replicas = 0
replica_port = None
async def handle_ping(writer):
    writer.write('+PONG\r\n'.encode())
    await writer.drain()

async def handle_echo(data, writer):
    resp = data[-2]
    writer.write(f"${len(resp)}\r\n{resp}\r\n".encode())
    await writer.drain()

async def handle_set(data, writer, store):
    global set_cmd
    print("I'm in set")
    key = data[4]
    value = data[6]
              
    if len(data) == 12:
        # Expire = current time in ms + additional time
        expire = int(time.time_ns() // 10**6) + int(data[10])
    else:
        expire = None
                
    store[key] = Item(value, expire) 
    set_cmd = True

    writer.write("+OK\r\n".encode())
    await writer.drain()

async def handle_get(data_split, store, dir, dbfilename, Item, writer):
    if dir and dbfilename:
        toggle_find_value(True)
        result, store = get_value_from_rdb(dir, dbfilename)
        key = data_split[4]
        store_item = store.get(key)
                  
                    
        time_now = time.time()
        print(f"time now {time_now}")
        print(f"store {store}")
        print(f"storeitem exp {store_item.expiry}")
        is_expired = (
            True
            if (store_item.expiry and store_item.expiry < time_now)
            else False
        )
        if not is_expired:
            writer.write(
                f"${len(store_item.value)}\r\n{store_item.value}\r\n".encode()
            )
        else:
            writer.write("$-1\r\n".encode())
        await writer.drain()
                    
                    
    else:
        key = data_split[4]
        store_item: Optional[Item] = store.get(key)
        print(f"store {store}")

        if (
             # If store and value exists
            store
            and store_item.value
            and (
                store_item.expiry is None # No expiry
                or (
                    # Expiry is in the future
                    store_item.expiry is not None
                    and store_item.expiry > (time.time_ns() // 10**6)
                )
            )
        ):
            writer.write(f"${len(store_item.value)}\r\n{store_item.value}\r\n".encode())
        else:
            writer.write("$-1\r\n".encode())
        await writer.drain()

def get_value_from_rdb(dir, dbfilename):
    print("im in get value from rdb")
    toggle_find_value(True)
    if dir and dbfilename:
        print(f"dir: {dir} , filename: {dbfilename}")
        # Construct full path to RDB file
        rdb_file_path = os.path.join(dir, dbfilename)
        if os.path.exists(rdb_file_path):
            # Open file and read its content
            with open(rdb_file_path, "rb") as rdb_file:
                rdb_content = str(rdb_file.read())
                print(f"rdb content: {rdb_content}")
                if rdb_content:
                    # Parse content to extract keys
                    result, store = parse_redis_file_format(rdb_content)
                    print(f"result from parse value: {result}")
                    
                    return result, store
    # If RDB file doesn't exist or no args provided
    return "*0\r\n".encode()

async def handle_info(data, args, writer):
    info_type = data[4] if len(data) == 6 else None
    if info_type and info_type == "replication":
        role = "slave" if args.replicaof else "master"
        role_str = f"role:{role}"
        if role == "master":
            master_replid = "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
            master_repl_offset = "master_repl_offset:0"
            master_info_str = f"{role_str}\n{master_replid}\n{master_repl_offset}"
            writer.write(f"${len(master_info_str)}\r\n{master_info_str}\r\n".encode())
        else:
            writer.write(f"${len(role_str)}\r\n{role_str}\r\n".encode())
        await  writer.drain()
    else:
        writer.write("$-1\r\n".encode())
        await writer.drain()

async def handle_replconf(data, writer):
    global ack_replicas, replica_port
    print("i am in replconf")
    response = ""
    if "listening-port" in data:
        print("is listening port")
        replica_port = data[6]
        response = '+OK\r\n'
               
    elif "capa" in data:
        response = '+OK\r\n'
    elif data[4] == "ACK":
        # Increase num of acknowledged replicas
        ack_replicas +=1             
        print(f"Increasing number of acknowledged replicas to: {ack_replicas}")
                    
    if response:
        writer.write(response.encode())
        await writer.drain()
    return replica_port

async def handle_psync(replica_writers, writer):
    fullresync = '+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n'
    # Send empty RDB file
    rdb_hex = '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2'
    binary_data = bytes.fromhex(rdb_hex)
    header = f"${len(binary_data)}\r\n"

    writer.write(fullresync.encode())
    await writer.drain()
    writer.write(header.encode() + binary_data)
    await writer.drain()
    replica_writers.append(writer)
    await writer.drain()

async def handle_wait(data, replica_writers, writer):
    # This command waits for a specified number of replicas to acknowledge the received command.
    # It sends REPLCONF GETACK commands to all replicas, then waits for the specified number of acknowledgments
    # or until the given timeout expires. Finally, it returns the number of acknowledgments received to the client.
    global set_cmdm, ack_replicas
    num_replicas_created = int(data[4])
    timeout = float(data[6]) / 1000  # Convert to seconds
    # Start timing and capture the current number of acknowledgments to avoid double counting
    start = time.time()
    initial_ack_count = ack_replicas
    # Send the REPLCONF GETACK command to all replicas
    print(" i am in wait replconf")
    print(f'value of set cmd {set_cmd}')
    for replica in replica_writers:
        replica.write(
            "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".encode()
        )
        await writer.drain()
    # Continuously check if the required number of replicas have acknowledged
    # or if the timeout has been reached
    while (ack_replicas - initial_ack_count < num_replicas_created) and (
        (time.time() - start) < timeout
    ):
        current_time = time.time()
        elapsed_time = current_time - start
        print(
            f"NUMACKS: {ack_replicas - initial_ack_count} of {num_replicas_created}, "
            f"elapsed_time: {elapsed_time:.4f} seconds, "
            f"timeout: {timeout:.4f} seconds"
        )
        await asyncio.sleep(
            0.005
        )  # Short sleep to avoid busy-waiting and allow other tasks to run
    # Calculate the final number of acknowledgments received within the timeout period
    final_acks = ack_replicas - initial_ack_count
    # Send the result back to the client: either the number of acknowledgments received
    # or the total number of replica writers if a different command (like SET) was issued
    
    writer.write(
        f":{final_acks if set_cmd else len(replica_writers) }\r\n".encode()
    )

async def handle_config(data, dir, dbfilename, writer):
    if "GET" in data:
        print("in config get")
        if "dir" in data:
            response = f"*2\r\n$3\r\ndir\r\n${len(dir)}\r\n{dir}\r\n"
        elif "dbfilename" in data:
            response = f"*2\r\n$10\r\ndbfilenamer\r\n${len(dbfilename)}\r\n{dbfilename}\r\n"
        writer.write(response.encode())

async def handle_keys(data, dir, dbfilename, writer):
    # Return all keys in database
    if "*" in data:
        keys = get_keys_from_rdb(dir, dbfilename)
        print(f"this is the keys {keys}")
        print("end of keyd")
        writer.write(keys)
        await writer.drain()

def get_keys_from_rdb(dir, dbfilename):
    print("im in get keys from rdb")
    if dir and dbfilename:
        print(f"dir: {dir} , filename: {dbfilename}")
        # Construct full path to RDB file
        rdb_file_path = os.path.join(dir, dbfilename)
        if os.path.exists(rdb_file_path):
            # Open file and read its content
            with open(rdb_file_path, "rb") as rdb_file:
                rdb_content = str(rdb_file.read())
                print(f"rdb content: {rdb_content}")
                if rdb_content:
                    # Parse content to extract keys

                    result, store = parse_redis_file_format(rdb_content)
                    print(f"eyoo {result}")
                    response = ""
                    for item in result:
                        response += f"${len(item)}\r\n{item}\r\n"
                    
                    return f"*{len(result)}\r\n{response}".encode()
    # If RDB file doesn't exist or no args provided
    return "*0\r\n".encode()

async def handle_type(data, stream_store, store, writer):
    print("IM IN TYPE")
    # Returns the type of a value stored at a given key
    key = data[4]
    store_item = None
    stream_store_item = None
    print(f"this is stream store {stream_store}")
    if store.get(key):
        store_item = store.get(key)
    elif key in stream_store.entries:
        stream_store_item = stream_store.entries[key]
    else:
        writer.write("+none\r\n".encode())
        await writer.drain()
                    
    if store_item or stream_store_item:
        if store_item:
            type_of_value = type(store_item.value)
                    
            if type_of_value == str:
                type_of_value = "string"
            elif type_of_value == list:
                type_of_value = "list"
            elif type_of_value == set:
                type_of_value = "set"
        elif stream_store_item:
            if isinstance(stream_store_item, StreamEntry):
                type_of_value = "stream"
                    
                        
        writer.write(f"+{type_of_value}\r\n".encode())
        await writer.drain()

async def handle_xadd(data, stream_store, writer):
    print(f'this is data split {data}')
    entry_id = data[6]
    entry_data = {}
    key = data[8]
    value = data[10]
    entry_data[key] = value
    new_entry = StreamEntry(entry_id, entry_data)
    
                
    stream_key = StreamEntries(data[4])
    print(f"type of stream key {type(stream_key)}")
    print(f"stream key output {stream_key}")
    stream_store.entries[stream_key.entries] = new_entry
    print(f"this is stream store {stream_store}")
    writer.write(f"$3\r\n{entry_id}\r\n".encode())
    await writer.drain()