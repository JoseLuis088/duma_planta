import paramiko

HOST = "172.168.10.106"
USER = "warehouseai"
PASS = "Ecosat201."

def main():
    ssh = None
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(HOST, username=USER, password=PASS)
        
        # Fetch logs
        stdin, stdout, stderr = ssh.exec_command("docker logs --tail 200 duma_planta")
        logs = stdout.read().decode() + stderr.read().decode()
        
        # Save to local file
        with open("vm_logs.txt", "w", encoding="utf-8") as f:
            f.write(logs)
            
        # Also fetch cpu info
        stdin, stdout, stderr = ssh.exec_command("cat /proc/cpuinfo | grep 'model name' | head -n 1")
        cpu = stdout.read().decode().strip()
        print(f"CPU: {cpu}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if ssh: ssh.close()

if __name__ == "__main__":
    main()
