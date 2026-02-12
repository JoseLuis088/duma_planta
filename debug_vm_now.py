import paramiko
import sys

HOST = "172.168.10.106"
USER = "warehouseai"
PASS = "Ecosat201."

def main():
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print(f"DEBUG_START: Connecting to {HOST}")
        ssh.connect(HOST, username=USER, password=PASS)
        print("DEBUG_CONNECTED")
        
        cmds = [
            "docker ps -a --filter name=duma_planta",
            "docker logs --tail 50 duma_planta"
        ]
        
        for cmd in cmds:
            print(f"\nDEBUG_CMD: {cmd}")
            stdin, stdout, stderr = ssh.exec_command(cmd)
            print(stdout.read().decode())
            print(stderr.read().decode())
            
        ssh.close()
        print("DEBUG_END")
    except Exception as e:
        print(f"DEBUG_ERROR: {e}")

if __name__ == "__main__":
    main()
