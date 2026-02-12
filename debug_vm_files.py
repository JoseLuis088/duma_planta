import paramiko
import sys

HOST = "172.168.10.106"
USER = "warehouseai"
PASS = "Ecosat201."

def main():
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(HOST, username=USER, password=PASS)
        
        cmds = [
            "ls -lh duma_planta/static/index.html",
            "stat duma_planta/static/index.html",
            "head -n 5 duma_planta/static/index.html"
        ]
        
        for cmd in cmds:
            print(f"\nDEBUG_CMD: {cmd}")
            stdin, stdout, stderr = ssh.exec_command(cmd)
            print(stdout.read().decode())
            print(stderr.read().decode())
            
        ssh.close()
    except Exception as e:
        print(f"DEBUG_ERROR: {e}")

if __name__ == "__main__":
    main()
