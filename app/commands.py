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
        print('a key in normal store')
        store_item = store.get(key)
    elif key in stream_store.entries:
        print('a key in streamstore')
        stream_store_item = stream_store.entries[key]
        print(stream_store_item)
    else:
        print('no key in any of the stores')
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
            if isinstance(stream_store_item, StreamEntry) or isinstance(stream_store_item, StreamEntries):
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
    xadd_done = False
    stream_name = data[4]
    
    if len(entry_id) == 1:
        if entry_id == "*":
            new_ms = round(time.time()*1000)
            new_seq = 0
    else:
        new_ms, new_seq = entry_id.split('-')
        new_ms = int(new_ms)
    print(f'first store {stream_store} and done? {xadd_done}')
    if not stream_store.entries:
        if new_ms > 0 and new_seq == "*":
            new_seq = 0
            
        if new_ms == 0:
            new_seq = 1

        entry_id = f"{new_ms}-{new_seq}"
        new_entry = StreamEntry(entry_id, entry_data)
        print('hello store was empty')
        print(new_entry)
        await add_entry(new_entry, entry_id, stream_name, stream_store, writer)
        xadd_done = True

    
    
    if not xadd_done and stream_store.entries:
        print(f"len of store {len(stream_store.entries)}")
        # Last entry in store
        prev_entry = list(stream_store.entries.values())[-1]
        print(f'this is prev entry id {prev_entry.entries[-1].id}')
        last_ms, last_seq = map(int, prev_entry.entries[-1].id.split('-'))
        print(last_ms, new_ms, last_seq, new_seq)
        
        if new_seq == "*":
            if new_ms > last_ms:
                new_seq = 0
            else:
                new_seq = last_seq + 1
        else:
            new_seq = int(new_seq)

        if new_ms > last_ms:
            entry_id = f"{new_ms}-{new_seq}"
            new_entry = StreamEntry(entry_id, entry_data)
            await add_entry(new_entry, entry_id, stream_name, stream_store, writer)
        elif new_ms == last_ms and new_seq > last_seq:
                entry_id = f"{new_ms}-{new_seq}"
                new_entry = StreamEntry(entry_id, entry_data)
                await add_entry(new_entry, entry_id, stream_name, stream_store, writer)
        elif new_ms == 0 and new_seq == 0:
            writer.write("-ERR The ID specified in XADD must be greater than 0-0\r\n".encode())
            await writer.drain()
        else:
            writer.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".encode())
            await writer.drain()
    elif not xadd_done:
        print('eyooo')
        new_entry = StreamEntry(entry_id, entry_data)
        await add_entry(new_entry, entry_id, stream_name, stream_store, writer)
        


async def add_entry(new_entry, entry_id, stream_name, stream_store, writer):
    stream_key = StreamEntries(entries=[])
    print(f'this is stream key {stream_key}')
    
    if stream_name not in stream_store.entries:
        stream_store.entries[stream_name] = stream_key
    
    stream_store.entries[stream_name].entries.append(new_entry)
    print(stream_store)
    writer.write(f"${len(entry_id)}\r\n{entry_id}\r\n".encode())
    await writer.drain()

async def handle_xrange(data, stream_store, writer):
    print("from handle xrange")
    start = data[6]
    end = data[8]
    stream_key = data[4]
    
    if start == "-":
        start = stream_store.entries[stream_key].entries[0].id

    if end == "+":
        end = stream_store.entries[stream_key].entries[-1].id
    if start is None:
        start = stream_store.entries[stream_key].entries[0].id
    if end is None:
        end = stream_store.entries[stream_key].entries[-1].id
    
    # Result will be RESP array
    res = "*0\r\n" 
    
    matching_entries = []
    
    # Filter entries by start and end ID
    for entry in stream_store.entries[stream_key].entries:
        entry_id = entry.id
        # Check if the entry is within the start and end range (inclusive)
        if start <= entry_id <= end:
            matching_entries.append(entry)
    # Add length matching entries
    res = f"*{len(matching_entries)}\r\n"
    # Add length of data
    res += f"*{len(matching_entries)}\r\n"

    for entry in matching_entries:
        # Add entry ID
        res += f"${len(entry_id)}\r\n{entry.id}\r\n"
        # Add the number of fields in "data"
        res += f"*{len(entry.data) * 2}\r\n"  # Each key-value pair counts as 2 elements in RESP
        
        # Loop through the data dictionary of the current entry
        for data_key, data_value in entry.data.items():
            # Add the key
            res += f"${len(data_key)}\r\n{data_key}\r\n"
            # Add the value
            res += f"${len(data_value)}\r\n{data_value}\r\n"
        # Add length of data 
        res += f"*{len(matching_entries)}\r\n"
   
    writer.write(res.encode())
    await writer.drain()

async def handle_xread(data, stream_store, writer):
    print("from handle_xread")
    print(data)

    # # Initialize response
    # if len(data) > 10:
    #     res = f"*2\r\n"  # Multiple streams
    # else:
    #     res = f"*1\r\n"  # Single stream
    res = ""
    block = False  # Initialize block as False
    blocking = False
    new_entry = "ignore"

    # Handle blocking
    if "block" in data:
        block = True
        time_block = int(data[data.index("block") + 2]) / 1000
        print("Blocking for:", time_block)

        initial_stream_state = {
            stream_key: len(stream_entries.entries)
            for stream_key, stream_entries in stream_store.entries.items()
        }

        # Handle the case for "BLOCK 0"
        if time_block == 0:
            blocking = True
            # Continuously check for new entries
            while new_entry == "ignore":
                await asyncio.sleep(0.1)  # Polling interval (100 ms)
                for stream_key, stream_entries in stream_store.entries.items():
                    if len(stream_entries.entries) > initial_stream_state[stream_key]:
                        new_entry = "yes"
                        break
        else:
            # Handle regular block with a time limit
            await asyncio.sleep(time_block)  # Use await for non-blocking sleep
            print("Not sleeping anymore")
            # Check if there is a new entry
            for stream_key, stream_entries in stream_store.entries.items():
                if len(stream_entries.entries) > initial_stream_state[stream_key]:
                    new_entry = "yes"
                    break
            else:
                new_entry = "no"

    stream_count = (len(data) - 6) // 2

    print(f'thisis stream cnt {stream_count}')
    n = 0
    i = 0

    if data[4] == "streams":
        # If multiple streams
        if len(data) > 10:
            res = f"*2\r\n"
        # If single stream
        else:
            res = f"*1\r\n"
        stream_count = (len(data) - 6) // 2
        n = 0
        i = 0
        # Loop through each stream and last ID
        while i < stream_count:
            if len(data) > 10:
                stream_key = data[6 + n]
                id = data[10 + n]
                n = 2
                i += 1
            else:
                stream_key = data[6]
                id = data[8]
                i = stream_count
            matching_entries = []
            print(f"Processing key: {stream_key}, id: {id}")
            # Check if the stream exists and filter entries by last ID
            if stream_key in stream_store.entries:
                for entry in stream_store.entries[stream_key].entries:
                    entry_id = entry.id
                    if id < entry.id:  # Match entries with IDs greater than last_id
                        matching_entries.append(entry)
            # Construct response for the current stream
            res += f"*{len(stream_store.entries[stream_key].entries[0].__dict__)}\r\n"
            res += f"${len(stream_key)}\r\n{stream_key}\r\n"
            res += f"*{len(matching_entries)}\r\n"
            res += f"*{len(stream_store.entries[stream_key].entries[0].__dict__)}\r\n"
    
            for entry in matching_entries:
                # Add entry ID
                res += f"${len(entry_id)}\r\n{entry.id}\r\n"
                # Add the number of fields in "data"
                res += f"*{len(entry.data) * 2}\r\n"  # Each key-value pair counts as 2 elements in RESP
                # Loop through the data dictionary of the current entry
                for data_key, data_value in entry.data.items():
                    # Add the key
                    res += f"${len(data_key)}\r\n{data_key}\r\n"
                    # Add the value
                    res += f"${len(data_value)}\r\n{data_value}\r\n"
        # Write the complete response
        print(f"This is res: {res.encode()}")
        writer.write(res.encode())
        await writer.drain()
    # Loop through each stream and last ID
    else:
        while i < stream_count:
            if block and new_entry == "yes" and not blocking:
                for stream_key, stream_entries in stream_store.entries.items():
                    # Check if there are entries in the stream
                    if stream_entries.entries:
                        last_entry = stream_entries.entries[-1]  # Accessing the last StreamEntry

                        print(f"Stream: {stream_key}")
                        print(f"Last Entry ID: {last_entry.id}")  # Output the ID of the last entry
                        print(f"Last Entry Data: {last_entry.data}")  # Output the data of the last entry
                        # Construct the response for the last entry
                        res = f"*1\r\n*2\r\n${len(stream_key)}\r\n{stream_key}\r\n*1\r\n*2\r\n"
                        res += f"${len(last_entry.id)}\r\n{last_entry.id}\r\n"

                        # Add the fields and values of the last entry
                        res += f"*{len(last_entry.data) * 2}\r\n"
                        for key, value in last_entry.data.items():
                            res += f"${len(key)}\r\n{key}\r\n"
                            res += f"${len(value)}\r\n{value}\r\n"

                await writer.drain()
                writer.write(res.encode())
                await writer.drain()
                return  # Exit the function to prevent sending the response twice

            if len(data) > 10:
                stream_key = data[6 + n]
                id = data[10 + n]
                n = 2
                i += 1
            else:
                stream_key = data[6]
                id = data[8]
                i = stream_count

            # Adjust for BLOCK 0 case if it's present
            if block and time_block == 0:
                stream_key = data[10]
                id = data[12]

            matching_entries = []
            print(f'Processing key: {stream_key}, last ID: {id}')

            # Check if the stream exists and filter entries by last ID
            if stream_key in stream_store.entries:
                for entry in stream_store.entries[stream_key].entries:
                    entry_id = entry.id
                    if id < entry.id:  # Match entries with IDs greater than last_id
                        matching_entries.append(entry)
            
            print(f'this is matching {matching_entries}')
            # Construct response for the current stream

            
            if matching_entries:
                res += f"*1\r\n*2\r\n"
                res += f"${len(stream_key)}\r\n{stream_key}\r\n"
                res += f"*{len(matching_entries)}\r\n"

                # Loop through matching entries
                for entry in matching_entries:
                    res += f"*2\r\n"
                    res += f"${len(entry.id)}\r\n{entry.id}\r\n"
                    res += f"*{len(entry.data) * 2}\r\n"  # Each key-value pair counts as 2 elements in RESP
                    for data_key, data_value in entry.data.items():
                        res += f"${len(data_key)}\r\n{data_key}\r\n"
                        res += f"${len(data_value)}\r\n{data_value}\r\n"
                writer.write(res.encode())
                await writer.drain()
        
                
    # If no new entry was added, return null bulk string
    if new_entry == "no":
        res = "$-1\r\n"

    # Write the complete response
    print(f'This is res: {res.encode()}')
    
    writer.write(res.encode())
    await writer.drain()
