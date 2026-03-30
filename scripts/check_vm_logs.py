import paramiko
import sys

# Configuration from deploy_vm.py
HOST = "172.168.10.106"
USER = "warehouseai"
PASS = "Ecosat201."
APP_NAME = "duma_planta"

def main():
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(HOST, username=USER, password=PASS)
        
        # Get the last 50 lines of docker logs
        cmd = f"echo '{PASS}' | sudo -S docker logs --tail 50 {APP_NAME}"
        print(f"Running: {cmd}")
        stdin, stdout, stderr = ssh.exec_command(cmd)
        
        print("\n--- STDOUT ---")
        print(stdout.read().decode())
        print("\n--- STDERR ---")
        print(stderr.read().decode())
        
        ssh.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
