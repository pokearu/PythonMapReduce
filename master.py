import kvstore as kv                         
import subprocess

kv_conn = kv.get_store_connection()

def process_data_file(file_path: str):
    data_input = open(file_path, 'r')
    count = 0
    while True:
        count += 1
        line = data_input.readline()
        if not line:
            # end of file is reached
            break
        line = line.strip()
        print("Line {}: {}".format(count, line))
        size = len(line.encode())
        kv.append_command(kv_conn,"hello",size,line)
    data_input.close()

def main():
    process_data_file(r'myfile.txt')
    completed = subprocess.run(['python', 'mapper_node.py'])
    kv.close_store_connection(kv_conn)


if __name__ == "__main__":
    main()
