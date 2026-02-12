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
            "curl -I http://localhost:80",
            "curl -s http://localhost:80 | wc -c"
        ]
        
        for cmd in cmds:
            print(f"\nDEBUG_CMD: {cmd}")
            stdin, stdout, stderr = ssh.exec_command(cmd)
            # Use stdout.read().decode('utf-8', errors='ignore') to avoid crashes
            out = stdout.read().decode('utf-8', errors='ignore')
            err = stderr.read().decode('utf-8', errors='ignore')
            print(out)
            print(err)
            
        ssh.close()
    except Exception as e:
        print(f"DEBUG_ERROR: {e}")

if __name__ == "__main__":
    main()
