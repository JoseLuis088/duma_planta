import paramiko
import time

HOST = "172.168.10.106"
USER = "warehouseai"
PASS = "Ecosat201."

def exec_print(ssh, cmd, title):
    print(f"\n--- {title} ---")
    try:
        stdin, stdout, stderr = ssh.exec_command(cmd)
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        if out: print(f"STDOUT:\n{out}")
        if err: print(f"STDERR:\n{err}")
    except Exception as e:
        print(f"Error executing {cmd}: {e}")

def main():
    ssh = None
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print(f"Connecting to {HOST}...")
        ssh.connect(HOST, username=USER, password=PASS)
        
        exec_print(ssh, "docker ps -a", "Docker Containers")
        exec_print(ssh, "docker logs --tail 50 duma_planta", "Container Logs")
        exec_print(ssh, "netstat -tuln | grep 80", "Listening Ports (80)")
        exec_print(ssh, "curl -vvv http://127.0.0.1:80", "Curl 127.0.0.1:80")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if ssh: ssh.close()

if __name__ == "__main__":
    main()
