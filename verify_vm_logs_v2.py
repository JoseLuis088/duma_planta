import paramiko
import time

HOST = "172.168.10.106"
USER = "warehouseai"
PASS = "Ecosat201."
APP_NAME = "duma_planta"

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, username=USER, password=PASS)
    
    print("\n--- Checking Container Logs ---")
    stdin, stdout, stderr = ssh.exec_command(f"echo '{PASS}' | sudo -S docker logs --tail 100 {APP_NAME}")
    print(stdout.read().decode())
    print(stderr.read().decode())
    
    ssh.close()

if __name__ == "__main__":
    main()
