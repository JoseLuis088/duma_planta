import paramiko
import os

HOST = "172.168.10.106"
USER = "warehouseai"
PASS = "Ecosat201."
APP_NAME = "duma_planta"

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(HOST, username=USER, password=PASS)
        # Higher tail and capture everything
        stdin, stdout, stderr = ssh.exec_command(f"sudo -S docker logs {APP_NAME} 2>&1 | tail -n 100")
        stdin.write(PASS + '\n')
        stdin.flush()
        
        logs = stdout.read().decode('utf-8', errors='ignore')
        with open("raw_vm_logs.txt", "w", encoding='utf-8') as f:
            f.write(logs)
        print("Logs saved to raw_vm_logs.txt")
        ssh.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
